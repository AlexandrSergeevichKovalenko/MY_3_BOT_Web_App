import { forwardRef, useCallback, useEffect, useImperativeHandle, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { viewportHeight } from './modalFit.js';

// Поиск по словам — отдельным окном на весь экран.
//
// Зачем окно, а не поле в списке: в «Библиотеке» над списком стоят вкладки, счётчик,
// панель выборки и папки. На маленьком телефоне клавиатура съедает низ, и от результата
// не остаётся НИ ОДНОЙ строки — человек печатает вслепую. В окне остаётся только поле и
// найденное, поэтому под клавиатурой помещается 6–7 строк.
//
// Ищем в ДВУХ местах сразу: сначала свои слова, следом — общий словарь (всё, что у нас
// уже разобрано). Второй список открыт всем и не зависит от подписки на базовый словарь:
// смысл ровно в том, чтобы человек нашёл слово у нас и не заказывал платный разбор того,
// что мы уже знаем.
//
// Высоту берём у Telegram (`viewportHeight` — то же правило, что у модалок и интерактивов):
// в шторке `innerHeight` врёт, и низ окна уехал бы под край видимой части.
//
// Окно ВСЕГДА в разметке (когда закрыто — прозрачное и не ловит нажатия). Так сделано
// ради клавиатуры: WebKit открывает её только если `focus()` вызван прямо в обработчике
// нажатия, а поле к этому моменту уже существует. Раньше поле появлялось кадром позже, и
// человеку приходилось тапать второй раз.

const RECENT_KEY = 'vocab_search_recent_v1';
const RECENT_MAX = 6;
const PAGE_SIZE = 60;
const POOL_PAGE_SIZE = 40;
const DEBOUNCE_MS = 250;

const ARTICLE_RE = /^(der|die|das|ein|eine|einen|einem|einer|eines)\s+/i;

/** Основа слова: артикль в запросе и в карточке игнорируем — ровно как это делает бэкенд. */
function stem(text) {
  return String(text || '').replace(ARTICLE_RE, '').trim().toLowerCase();
}

function readRecent() {
  try {
    const raw = JSON.parse(localStorage.getItem(RECENT_KEY) || '[]');
    return Array.isArray(raw) ? raw.filter((x) => typeof x === 'string' && x.trim()).slice(0, RECENT_MAX) : [];
  } catch (_error) {
    return [];
  }
}

function writeRecent(list) {
  try {
    localStorage.setItem(RECENT_KEY, JSON.stringify(list.slice(0, RECENT_MAX)));
  } catch (_error) {
    /* приватный режим — просто без истории */
  }
}

/** Подсветка совпавшего куска. Возвращает готовые узлы, без dangerouslySetInnerHTML. */
function highlight(text, needle) {
  const source = String(text || '');
  if (!needle) return source;
  const at = source.toLowerCase().indexOf(needle);
  if (at < 0) return source;
  return (
    <>
      {source.slice(0, at)}
      <mark className="vso-hit">{source.slice(at, at + needle.length)}</mark>
      {source.slice(at + needle.length)}
    </>
  );
}

/** Слово с окрашенным артиклем — как в списке библиотеки. */
function WordText({ word, needle }) {
  const match = /^(der|die|das)\s+/i.exec(String(word || ''));
  if (!match) return highlight(word, needle);
  const gender = match[1].toLowerCase() === 'der' ? 'm' : match[1].toLowerCase() === 'die' ? 'f' : 'n';
  return (
    <>
      <span className={`vocab-artikel is-${gender}`}>{String(word).slice(0, match[1].length)}</span>
      {highlight(String(word).slice(match[1].length), needle)}
    </>
  );
}

const VocabSearchOverlay = forwardRef(function VocabSearchOverlay({
  open,
  onClose,
  host,
  initData,
  fetchWithTimeout,
  tr,
  seedItems = [],
  totalCount = 0,
  folders = [],
  activeFolderKey = 'all',
  noFolderCount = 0,
  selectedIds = [],
  onToggleSelect,
  onOpenWord,
  onLookupInDictionary,
  onAddFromPool,
  resolveFolderIconLabel,
  sanitizeBilingualTargetText,
}, ref) {
  const [query, setQuery] = useState('');
  const [scope, setScope] = useState('all');
  const [items, setItems] = useState([]);
  const [total, setTotal] = useState(0);
  const [poolItems, setPoolItems] = useState([]);
  const [failed, setFailed] = useState(false);
  const [recent, setRecent] = useState(() => readRecent());
  const [height, setHeight] = useState(() => (typeof window === 'undefined' ? 0 : viewportHeight()));
  const [addedIds, setAddedIds] = useState([]);
  const [addingId, setAddingId] = useState('');
  // Запрос, на который сервер уже ответил. Пока он не совпал с набранным, показываем
  // подходящее из уже загруженных слов — список шевелится с первой буквы.
  const [servedQuery, setServedQuery] = useState('');

  const inputRef = useRef(null);
  const requestRef = useRef(0);

  const needle = stem(query);
  const hasServerAnswer = Boolean(query.trim()) && servedQuery === `${scope}::${query.trim()}`;
  const selectedSet = useMemo(() => new Set((selectedIds || []).map(Number)), [selectedIds]);
  const addedSet = useMemo(() => new Set(addedIds.map(String)), [addedIds]);

  // Клавиатура WebKit открывается только если фокус поставлен прямо в обработчике
  // нажатия. Поэтому фокус ставит тот, кто открывает окно, а не эффект после рендера.
  useImperativeHandle(ref, () => ({
    focusInput() {
      try { inputRef.current?.focus(); } catch (_error) { /* ignore */ }
    },
  }), []);

  // ── что показываем в строке ────────────────────────────────────────────────
  const describe = useCallback((item) => {
    const word = item.display_word || item.word_de || item.word_ru || '—';
    const translation = sanitizeBilingualTargetText
      ? sanitizeBilingualTargetText(
          word,
          item.display_translation || item.translation_ru || item.word_ru || '',
          item.target_lang || '',
        )
      : (item.display_translation || item.translation_ru || '');
    const folder = (folders || []).find((f) => f.id === item.folder_id) || null;
    return { word, translation, folder };
  }, [folders, sanitizeBilingualTargetText]);

  // ── порядок: точное слово → начинается с набранного → всё остальное ────────
  // Бэкенд отдаёт по дате добавления, поэтому ровно то слово, которое человек набрал,
  // оказывалось десятым в списке. Сортируем на месте — это дешевле, чем менять SQL.
  const rank = useCallback((item) => {
    const word = stem(item.display_word || item.word_de || item.word_ru || '');
    const native = String(item.display_translation || item.translation_ru || item.word_ru || '').toLowerCase();
    if (!needle) return 4;
    if (word === needle) return 0;
    if (word.startsWith(needle)) return 1;
    if (native.startsWith(needle)) return 2;
    if (word.includes(needle)) return 3;
    return 4;
  }, [needle]);

  const matchesLocally = useCallback((item) => {
    if (!needle) return false;
    const haystack = [
      item.display_word, item.word_de, item.word_ru,
      item.display_translation, item.translation_ru, item.translation_de,
    ];
    return haystack.some((value) => stem(value).includes(needle) || String(value || '').toLowerCase().includes(needle));
  }, [needle]);

  const inScope = useCallback((item) => {
    if (scope === 'all') return true;
    if (scope === 'none') return !item.folder_id;
    return String(item.folder_id) === String(scope);
  }, [scope]);

  // Первая буква не должна ждать сеть: те слова, что уже загружены в библиотеку,
  // фильтруем на месте и показываем сразу. Ответ сервера просто уточнит список.
  const shown = useMemo(() => {
    const source = hasServerAnswer ? items : (seedItems || []).filter(matchesLocally).filter(inScope);
    return [...source].sort((a, b) => {
      const diff = rank(a) - rank(b);
      if (diff !== 0) return diff;
      return stem(a.display_word || a.word_de || a.word_ru).localeCompare(stem(b.display_word || b.word_de || b.word_ru));
    });
  }, [items, seedItems, matchesLocally, inScope, rank, hasServerAnswer]);

  // Общий словарь показываем только тем, чего у человека нет. Сервер отсекает по связи
  // карточки с записью пула, но у старых карточек связи может не быть — поэтому здесь
  // ещё раз сверяем по самому слову.
  const poolShown = useMemo(() => {
    if (!hasServerAnswer) return [];
    const mine = new Set(shown.map((it) => stem(it.display_word || it.word_de || it.word_ru)));
    return poolItems.filter((it) => !mine.has(stem(it.display_word || it.word_de || it.word_ru)));
  }, [poolItems, shown, hasServerAnswer]);

  // ── запросы на сервер: пауза 250 мс + защита от гонки ответов ──────────────
  // Раньше каждая буква уходила отдельным запросом («M», «Mü», «Mün»…), а поздний
  // ответ на короткий запрос перезаписывал список для длинного — список «прыгал».
  useEffect(() => {
    if (!open) return undefined;
    const text = query.trim();
    if (!text) {
      setServedQuery('');
      setItems([]);
      setPoolItems([]);
      setTotal(0);
      setFailed(false);
      return undefined;
    }
    const ticket = requestRef.current + 1;
    requestRef.current = ticket;
    const timer = setTimeout(async () => {
      const askMine = fetchWithTimeout('/api/webapp/vocabulary/list', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          folder_id: scope === 'all' ? null : scope === 'none' ? -1 : Number(scope),
          search: text,
          sort: 'date_desc',
          limit: PAGE_SIZE,
          offset: 0,
        }),
      }, 15000).then((r) => (r.ok ? r.json() : Promise.reject(new Error('mine'))));

      const askPool = fetchWithTimeout('/api/webapp/vocabulary/pool-search', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, query: text, limit: POOL_PAGE_SIZE }),
      }, 15000).then((r) => (r.ok ? r.json() : Promise.reject(new Error('pool'))));

      const [mineResult, poolResult] = await Promise.allSettled([askMine, askPool]);
      if (ticket !== requestRef.current) return; // ответ на устаревший запрос

      if (mineResult.status === 'fulfilled') {
        const data = mineResult.value || {};
        setItems(Array.isArray(data.items) ? data.items : []);
        setTotal(Number(data.total || 0));
        setServedQuery(`${scope}::${text}`);
        setFailed(false);
      } else {
        setFailed(true);
      }
      // Общий словарь — дополнение: если он не ответил, свои слова всё равно показываем.
      setPoolItems(
        poolResult.status === 'fulfilled' && Array.isArray(poolResult.value?.items)
          ? poolResult.value.items
          : [],
      );
    }, DEBOUNCE_MS);
    return () => clearTimeout(timer);
  }, [open, query, scope, initData, fetchWithTimeout]);

  // ── окно ровно по видимой высоте, и при появлении клавиатуры тоже ──────────
  useEffect(() => {
    if (!open) return undefined;
    const apply = () => setHeight(viewportHeight());
    apply();
    const vv = window.visualViewport;
    vv?.addEventListener('resize', apply);
    vv?.addEventListener('scroll', apply);
    window.addEventListener('resize', apply);
    try { window.Telegram?.WebApp?.onEvent?.('viewportChanged', apply); } catch (_e) { /* ignore */ }
    return () => {
      vv?.removeEventListener('resize', apply);
      vv?.removeEventListener('scroll', apply);
      window.removeEventListener('resize', apply);
      try { window.Telegram?.WebApp?.offEvent?.('viewportChanged', apply); } catch (_e) { /* ignore */ }
    };
  }, [open]);

  useEffect(() => {
    if (!open) {
      setQuery('');
      setServedQuery('');
      setItems([]);
      setPoolItems([]);
      setAddedIds([]);
      setFailed(false);
      return undefined;
    }
    setScope('all');
    setRecent(readRecent());
    // Подстраховка для Android и браузера: там фокус из обработчика нажатия иногда
    // теряется на переключении экрана. В WebKit к этому моменту поле уже в фокусе.
    const timer = setTimeout(() => { try { inputRef.current?.focus(); } catch (_e) { /* ignore */ } }, 60);
    return () => clearTimeout(timer);
  }, [open]);

  const rememberQuery = useCallback((text) => {
    const clean = String(text || '').trim();
    if (clean.length < 2) return;
    const next = [clean, ...readRecent().filter((x) => x.toLowerCase() !== clean.toLowerCase())];
    writeRecent(next);
    setRecent(next.slice(0, RECENT_MAX));
  }, []);

  const openWord = useCallback((item) => {
    rememberQuery(query);
    onOpenWord?.(item);
  }, [onOpenWord, query, rememberQuery]);

  const addFromPool = useCallback(async (item) => {
    // Слово может прийти из слоя единиц или из общего пула — ключ у них разный.
    const entryId = String(item.unit_id ? `unit-${item.unit_id}` : (item.pool_entry_id || item.id || ''));
    if (!entryId || addedSet.has(entryId) || addingId) return;
    setAddingId(entryId);
    try {
      const ok = await onAddFromPool?.(item);
      if (ok) setAddedIds((prev) => [...prev, entryId]);
    } finally {
      setAddingId('');
    }
  }, [onAddFromPool, addedSet, addingId]);

  if (!host) return null;

  const activeFolder = (folders || []).find((f) => String(f.id) === String(activeFolderKey)) || null;
  const showScopeChips = activeFolderKey === 'none' || Boolean(activeFolder);
  const trimmed = query.trim();
  const truncated = hasServerAnswer && total > shown.length;
  const nothingAtAll = trimmed && !failed && hasServerAnswer && shown.length === 0 && poolShown.length === 0;

  return createPortal((
    <div
      className={`vso-overlay ${open ? '' : 'is-hidden'}`}
      style={{ height: height ? `${Math.round(height)}px` : undefined }}
      aria-hidden={open ? undefined : 'true'}
    >
      <div className="vso-top">
        <div className="vso-field">
          <div className="vso-input-wrap">
            <span className="vso-glass" aria-hidden="true">
              <svg viewBox="0 0 24 24" width="17" height="17" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
                <circle cx="11" cy="11" r="7" /><path d="m20 20-3.2-3.2" />
              </svg>
            </span>
            <input
              ref={inputRef}
              className="vso-input"
              type="search"
              inputMode="search"
              autoComplete="off"
              autoCorrect="off"
              spellCheck="false"
              enterKeyHint="search"
              tabIndex={open ? 0 : -1}
              value={query}
              placeholder={tr('Поиск по словам', 'Wörter suchen')}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') { e.preventDefault(); rememberQuery(query); e.currentTarget.blur(); }
                if (e.key === 'Escape') { e.preventDefault(); onClose?.(); }
              }}
            />
            {trimmed && (
              <button
                type="button"
                className="vso-clear"
                onClick={() => { setQuery(''); try { inputRef.current?.focus(); } catch (_e) { /* ignore */ } }}
                aria-label={tr('Стереть', 'Löschen')}
              >
                ×
              </button>
            )}
          </div>
          <button type="button" className="vso-cancel" onClick={() => onClose?.()}>
            {tr('Отмена', 'Abbrechen')}
          </button>
        </div>
        <div className="vso-meta">
          {/* Счётчик стоит у своего списка, а не в шапке: «Найдено 2 119» складывало
              свои слова и общий словарь — два разных мира, и число не значило ничего. */}
          <span className="vso-count">
            {trimmed
              ? tr('Поиск по словам', 'Wörter suchen')
              : `${totalCount} ${tr('слов у вас', 'Wörter bei dir')}`}
          </span>
          {showScopeChips && (
            <>
              <button
                type="button"
                className={`vso-chip ${scope === 'all' ? 'is-on' : ''}`}
                onClick={() => setScope('all')}
              >
                {tr('Везде', 'Überall')}
              </button>
              <button
                type="button"
                className={`vso-chip ${scope !== 'all' ? 'is-on' : ''}`}
                onClick={() => setScope(activeFolder ? String(activeFolder.id) : 'none')}
              >
                {activeFolder
                  ? `${resolveFolderIconLabel ? resolveFolderIconLabel(activeFolder.icon) : '📂'} ${activeFolder.name} · ${activeFolder.word_count ?? 0}`
                  : `🗂 ${tr('Без папки', 'Ohne Ordner')} · ${noFolderCount}`}
              </button>
            </>
          )}
        </div>
      </div>

      <div className="vso-results">
        {!trimmed && recent.length > 0 && (
          <>
            <div className="vso-section">{tr('Недавние', 'Zuletzt')}</div>
            <div className="vso-recent">
              {recent.map((word) => (
                <button key={word} type="button" className="vso-recent-chip" onClick={() => setQuery(word)}>
                  {word}
                </button>
              ))}
            </div>
          </>
        )}

        {trimmed && failed && shown.length === 0 && (
          <div className="vso-empty">
            <div className="vso-empty-title">{tr('Не удалось найти', 'Suche fehlgeschlagen')}</div>
            {tr('Проверьте связь и наберите слово ещё раз.', 'Prüfe die Verbindung und tippe das Wort erneut.')}
          </div>
        )}

        {nothingAtAll && (
          <div className="vso-empty">
            <div className="vso-empty-title">«{trimmed}» {tr('пока нет', 'gibt es noch nicht')}</div>
            {tr('Ни у вас, ни в общем словаре. Слово можно разобрать и сразу сохранить к себе.',
                'Weder bei dir noch im gemeinsamen Wörterbuch. Du kannst es nachschlagen und gleich speichern.')}
            <div>
              <button
                type="button"
                className="vso-cta"
                onClick={() => { rememberQuery(trimmed); onLookupInDictionary?.(trimmed); }}
              >
                {tr('Найти в словаре', 'Im Wörterbuch suchen')}
              </button>
            </div>
          </div>
        )}

        {trimmed && shown.length > 0 && (
          <div className="vso-section">
            {tr('Мои слова', 'Meine Wörter')} · {hasServerAnswer ? total : shown.length}
          </div>
        )}

        {shown.map((item) => {
          const { word, translation, folder } = describe(item);
          const picked = selectedSet.has(Number(item.id));
          return (
            <div key={`mine-${item.id}`} className="vso-row">
              <button
                type="button"
                className={`vso-check ${picked ? 'is-on' : ''}`}
                onClick={() => onToggleSelect?.(item.id)}
                aria-label={tr('Добавить слово в текущую выборку', 'Wort zur aktuellen Auswahl hinzufügen')}
              >
                {picked && (
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                    <polyline points="20 6 9 17 4 12" />
                  </svg>
                )}
              </button>
              <button type="button" className="vso-row-main" onClick={() => openWord(item)}>
                <span className="vso-row-body">
                  <span className="vso-word"><WordText word={word} needle={needle} /></span>
                  {translation && <span className="vso-trans">{highlight(translation, needle)}</span>}
                </span>
                {folder && (
                  <span className="vso-folder">
                    {resolveFolderIconLabel ? resolveFolderIconLabel(folder.icon) : '📂'} {folder.name}
                  </span>
                )}
              </button>
            </div>
          );
        })}

        {truncated && (
          <div className="vso-more">
            {tr('Показаны первые', 'Gezeigt: die ersten')} {shown.length} · {tr('уточните запрос', 'Suche eingrenzen')}
          </div>
        )}

        {poolShown.length > 0 && (
          <>
            <div className="vso-section vso-section-pool">
              {tr('В общем словаре', 'Im gemeinsamen Wörterbuch')} · {poolShown.length}
            </div>
            {poolShown.map((item) => {
              const { word, translation } = describe(item);
              const entryId = String(item.unit_id ? `unit-${item.unit_id}` : (item.pool_entry_id || item.id || ''));
              const added = addedSet.has(entryId);
              const busy = addingId === entryId;
              return (
                <div key={`pool-${entryId}`} className={`vso-row is-pool ${added ? 'is-added' : ''}`}>
                  <button
                    type="button"
                    className={`vso-add ${added ? 'is-on' : ''}`}
                    onClick={() => addFromPool(item)}
                    disabled={added || busy}
                    aria-label={added
                      ? tr('Слово уже у вас', 'Wort ist schon bei dir')
                      : tr('Добавить слово к себе', 'Wort zu mir hinzufügen')}
                  >
                    {added ? (
                      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                        <polyline points="20 6 9 17 4 12" />
                      </svg>
                    ) : busy ? '…' : '+'}
                  </button>
                  <button type="button" className="vso-row-main" onClick={() => openWord(item)}>
                    <span className="vso-row-body">
                      <span className="vso-word"><WordText word={word} needle={needle} /></span>
                      {translation && <span className="vso-trans">{highlight(translation, needle)}</span>}
                    </span>
                    <span className="vso-folder vso-folder-pool">
                      {added ? tr('у вас', 'bei dir') : tr('общий', 'gemeinsam')}
                    </span>
                  </button>
                </div>
              );
            })}
          </>
        )}
      </div>
    </div>
  ), host);
});

export default VocabSearchOverlay;
