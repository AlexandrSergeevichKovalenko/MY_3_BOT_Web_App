import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { api, haptic, SpeakButton, getInitData, getDictToken } from './WordBreakdown';
import { humanizeDictError } from './errors.js';
import { saveLookedUpWord, savePhraseWithTranslation } from './saveUtils';
import ProFeatureModal from '../components/ProFeatureModal';

/**
 * Вкладка «Отличия» — чем похожие слова отличаются друг от друга.
 *
 * Экран один на оба словаря: быстрый (DictionaryOverlay) и внутренний (App.jsx).
 * Цвета берутся из токенов вокруг (--dq-accent и соседи), поэтому «Фукс» получает
 * свой вид даром, а раскладка у двух словарей не может разъехаться — она одна.
 *
 * Чего здесь НЕТ и быть не должно: разбора, собранного на клиенте. Сервер либо
 * присылает готовые блоки, либо честно говорит, что слова не нашёл; додумывать
 * значение слова на фронте нельзя ничем.
 */

const MIN_WORDS = 2;
const MAX_WORDS = 4;

const CONTRAST_LABEL = {
  wrong: 'Так не говорят.',
  possible_but_different_meaning: 'Сказать можно, но смысл будет другим.',
  possible_but_different_register: 'Сказать можно, но это другой стиль.',
};

const REFLEXIVITY_LABEL = {
  both: 'бывает и возвратным, и невозвратным',
  only_reflexive: 'только возвратный (с sich)',
  only_non_reflexive: 'только невозвратный',
  not_applicable: '',
};

const INTERCHANGEABLE_LABEL = {
  no: 'Заменять нельзя',
  sometimes: 'Иногда заменяют',
  yes: 'Обычно заменяемы',
};

// Части речи показываем по-человечески: «Verb» ученику ничего не говорит.
const POS_LABEL = {
  noun: 'существительное', verb: 'глагол', adjective: 'прилагательное',
  adverb: 'наречие', pronoun: 'местоимение', preposition: 'предлог',
  conjunction: 'союз', phrase: 'выражение', participle: 'причастие',
  numeral: 'числительное', particle: 'частица', interjection: 'междометие',
};

// Слова не пересеклись по смыслу — это ответ, а не ошибка: человек мог свести
// «ausweisen» и «abschieben» наугад, и честнее сказать прямо.
const COMPARABLE_LABEL = {
  none: 'Общего смысла нет',
  one_sense: 'Пересекаются в одном значении',
};

// Сравниваемые слова внутри объяснения набираются жирным: глаз цепляется за то, о чём
// речь, и абзац читается за пару секунд. Текст при этом не меняется ни на букву —
// мы только выделяем то, что в нём уже написано.
function emphasize(text, words) {
  const source = String(text || '');
  const list = (words || []).map((w) => String(w || '').trim()).filter(Boolean);
  if (!source || !list.length) return source;
  const escaped = list
    .slice()
    .sort((a, b) => b.length - a.length)
    .map((w) => w.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
  const parts = source.split(new RegExp(`(${escaped.join('|')})`, 'gi'));
  return parts.map((part, i) => (
    list.some((w) => w.toLowerCase() === part.toLowerCase())
      ? <b className="wd-em" key={`em-${i}`}>{part}</b>
      : <React.Fragment key={`t-${i}`}>{part}</React.Fragment>
  ));
}

function normalizeCells(cells) {
  return cells.map((t) => String(t || '').trim()).filter(Boolean);
}

export default function WordDiff({ sharedToken = '', tts = null, onNeedFullAccess = null }) {
  const [cells, setCells] = useState(['', '']);
  const [phase, setPhase] = useState(sharedToken ? 'loading' : 'idle');
  const [result, setResult] = useState(null);
  const [missing, setMissing] = useState([]);
  const [error, setError] = useState('');
  const [limitReached, setLimitReached] = useState(false);
  const [history, setHistory] = useState([]);   // свои сравнения
  const [popular, setPopular] = useState([]);   // всё, что разбирали все, по частоте
  const [isAdmin, setIsAdmin] = useState(false);    // хозяин общей полки
  const [swiped, setSwiped] = useState('');         // строка, смахнутая влево
  // Списки сворачиваются: они растут, а рабочая часть экрана должна остаться видимой.
  // Свои сравнения открыты (их немного и они свои), общая полка свёрнута.
  const [openLists, setOpenLists] = useState({ mine: true, shelf: false });
  const [choices, setChoices] = useState([]);       // что имел в виду человек
  const [picks, setPicks] = useState({});           // его ответы: слово → прочтение
  const swipeRef = useRef({ key: '', x: 0 });
  const [saved, setSaved] = useState(() => new Set());
  const [sharing, setSharing] = useState(false);
  const [streaming, setStreaming] = useState(false); // разбор ещё дописывается
  const [stageText, setStageText] = useState('');    // что сервер делает прямо сейчас
  // Окно «нужен полный доступ». Рисует его САМ этот компонент, а не тот, кто его
  // вставил, — потому что оно должно быть одинаковым в обоих словарях. Раньше окно
  // было только во внутреннем словаре, а быстрый по тому же сигналу молча уносил
  // человека в Telegram: сигнал «нужен полный доступ» висел на кнопке внутри плашки,
  // а после переезда плашки в окно стал срабатывать сам. Владелец 27.08.2026 упёрся
  // ровно в это. Теперь наружу (в Telegram или на «Подписку») уходят ТОЛЬКО по нажатию
  // кнопки в этом окне.
  const [paywallOpen, setPaywallOpen] = useState(false);

  const filled = useMemo(() => normalizeCells(cells), [cells]);
  const canSubmit = filled.length >= MIN_WORDS && phase !== 'loading';
  const isGuest = Boolean(sharedToken);

  const loadHistory = useCallback(async () => {
    try {
      const data = await api('/api/webapp/dictionary/diff/history', { limit: 20 });
      setHistory(Array.isArray(data?.items) ? data.items : []);
      setPopular(Array.isArray(data?.popular) ? data.popular : []);
      setIsAdmin(data?.is_admin === true);
    } catch (_e) {
      // Списки — витрина, а не ответ на вопрос человека. Их отсутствие не должно мешать
      // сравнивать слова, поэтому здесь тихо. Ошибка самого разбора — громкая.
      setHistory([]);
      setPopular([]);
    }
  }, []);

  useEffect(() => {
    if (isGuest) return;
    void loadHistory();
  }, [isGuest, loadHistory]);

  // Гостевой показ: человек открыл чужую ссылку. Ввода нет, сохранений нет.
  useEffect(() => {
    if (!sharedToken) return;
    let alive = true;
    (async () => {
      try {
        const data = await api('/api/webapp/dictionary/diff/shared', { share_token: sharedToken });
        if (!alive) return;
        setResult(data);
        setPhase('ready');
      } catch (e) {
        if (!alive) return;
        setError(humanizeDictError(e));
        setPhase('error');
      }
    })();
    return () => { alive = false; };
  }, [sharedToken]);

  // Разбор приходит ПОТОКОМ: «Главное» появляется через пару секунд, остальные блоки
  // дописываются на глазах. Замер 25.08.2026: целиком пара собиралась 9–18 секунд, и
  // всё это время человек смотрел в пустой экран. Готовая пара приходит обычным JSON —
  // потока там нет и не нужно.
  // Выбор человека нужен внутри уже запущенного запроса, поэтому держим его в ссылке:
  // состояние обновится позже, а тело запроса собирается сейчас.
  const picksRef = useRef({});

  const runDiff = useCallback(async (words) => {
    setPhase('loading');
    setError('');
    setLimitReached(false);
    setMissing([]);
    setSaved(new Set());
    setStreaming(false);
    setStageText('');
    try {
      const token = getDictToken();
      const headers = { 'Content-Type': 'application/json', 'X-Telegram-InitData': getInitData() };
      if (token) headers['X-Dict-Token'] = token;
      const resp = await fetch('/api/webapp/dictionary/diff/stream', {
        method: 'POST',
        headers,
        body: JSON.stringify({
          initData: getInitData(), ...(token ? { dqt: token } : {}), words, picks: picksRef.current,
        }),
      });
      if (!resp.ok) {
        const body = await resp.json().catch(() => ({}));
        const err = new Error(body?.error || 'Fehler');
        err.status = resp.status;
        err.payload = body;
        throw err;
      }
      // Кеш, отказ и «слова не нашли» приходят обычным JSON.
      if ((resp.headers.get('Content-Type') || '').includes('application/json')) {
        const data = await resp.json().catch(() => ({}));
        if (data && data.ok === false && data.reason === 'not_found') {
          setMissing(Array.isArray(data.missing) ? data.missing : []);
          setPhase('missing');
          return;
        }
        if (data && data.ok === false && data.reason === 'choose_sense') {
          setChoices(Array.isArray(data.choices) ? data.choices : []);
          setPhase('choose');
          return;
        }
        if (data && data.ok === false && data.reason === 'paid_only') {
          // Платный отказ — не ошибка экрана, а разговор о деньгах, и место ему в
          // модальном окне (владелец 27.08.2026: «не должны занимать место на экране
          // под вот эту плашку»). Экран остаётся как был, человек закрывает окно и
          // продолжает. Серверную формулировку наружу не выносим: текст окна пишет
          // тот, кто это окно показывает.
          setPhase('idle');
          setPaywallOpen(true);
          return;
        }
        setResult(data);
        setPhase('ready');
        void loadHistory();
        return;
      }
      if (!resp.body || typeof resp.body.getReader !== 'function') throw new Error('stream unsupported');

      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      let streamError = '';
      setStreaming(true);

      const handleFrame = (block) => {
        let event = 'message';
        const dataLines = [];
        for (const line of block.split('\n')) {
          if (line.startsWith('event:')) event = line.slice(6).trim();
          else if (line.startsWith('data:')) dataLines.push(line.slice(5).trim());
        }
        if (!dataLines.length) return;
        let data;
        try { data = JSON.parse(dataLines.join('\n')); } catch (_e) { return; }
        if (event === 'stage') {
          // Шаги настоящие: сервер сообщает, что делает прямо сейчас.
          setStageText(String(data?.text || ''));
        } else if (event === 'refuse') {
          if (data && data.reason === 'not_found') {
            setMissing(Array.isArray(data.missing) ? data.missing : []);
            setPhase('missing');
          } else if (data && data.reason === 'choose_sense') {
            setChoices(Array.isArray(data.choices) ? data.choices : []);
            setPhase('choose');
          } else if (data && data.reason === 'paid_only') {
            // То же, что и в JSON-ветке выше: платный отказ живёт в модальном окне.
            setPhase('idle');
            setPaywallOpen(true);
          } else {
            setError(String(data?.message || data?.error || 'Не получилось разобрать.'));
            setLimitReached(String(data?.error || '') === 'free_limit_exceeded');
            setPhase('error');
          }
          streamError = '';
        } else if (event === 'section') {
          setResult((prev) => ({ ...(prev || {}), words, diff: data.diff || {} }));
          setPhase('ready');
        } else if (event === 'done') {
          setResult(data);
          setPhase('ready');
          setStreaming(false);
          void loadHistory();
        } else if (event === 'error') {
          streamError = String(data?.error || 'Не удалось разобрать отличия.');
        }
      };

      // eslint-disable-next-line no-constant-condition
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        let idx;
        while ((idx = buffer.indexOf('\n\n')) >= 0) {
          handleFrame(buffer.slice(0, idx));
          buffer = buffer.slice(idx + 2);
        }
      }
      if (buffer.trim()) handleFrame(buffer);
      setStreaming(false);
      if (streamError) {
        setError(streamError);
        setPhase('error');
      }
    } catch (e) {
      setStreaming(false);
      if (e && e.status === 429) {
        setLimitReached(true);
        setError(humanizeDictError(e));
        setPhase('error');
        return;
      }
      setError(humanizeDictError(e));
      setPhase('error');
    }
  }, [loadHistory]);

  const submit = useCallback(() => {
    const words = normalizeCells(cells);
    if (words.length < MIN_WORDS) return;
    haptic('light');
    void runDiff(words.slice(0, MAX_WORDS));
  }, [cells, runDiff]);

  const setCell = useCallback((index, value) => {
    setCells((prev) => prev.map((old, i) => (i === index ? value : old)));
  }, []);

  const addCell = useCallback(() => {
    setCells((prev) => (prev.length >= MAX_WORDS ? prev : [...prev, '']));
  }, []);

  const applySuggestion = useCallback((wrong, right) => {
    setCells((prev) => prev.map((t) => (String(t || '').trim() === wrong ? right : t)));
    setMissing([]);
    setPhase('idle');
  }, []);

  const openPair = useCallback((pairWords) => {
    const list = (Array.isArray(pairWords) ? pairWords : []).slice(0, MAX_WORDS);
    if (list.length < MIN_WORDS) return;
    setCells(list);
    void runDiff(list);
  }, [runDiff]);

  // Смахнуть строку влево, как в почте на телефоне: показывается «Удалить».
  // Работает и пальцем, и мышью — обработчики указателя одни на оба случая.
  const onRowPointerDown = useCallback((event, key) => {
    if (!isAdmin) return;
    swipeRef.current = { key, x: event.clientX };
  }, [isAdmin]);

  const onRowPointerUp = useCallback((event, key) => {
    if (!isAdmin || swipeRef.current.key !== key) return;
    const shift = event.clientX - swipeRef.current.x;
    swipeRef.current = { key: '', x: 0 };
    if (shift < -50) setSwiped(key);
    else if (shift > 30) setSwiped('');
  }, [isAdmin]);

  const removePair = useCallback(async (pairKey) => {
    if (!isAdmin || !pairKey) return;
    // Убираем из списка сразу: подтверждение с сервера придёт следом, а если не придёт —
    // строка вернётся на место и человек увидит текст ошибки.
    const before = popular;
    const beforeHistory = history;
    setPopular((prev) => prev.filter((row) => row.pair_key !== pairKey));
    setHistory((prev) => prev.filter((row) => row.pair_key !== pairKey));
    setSwiped('');
    haptic('ok');
    try {
      await api('/api/webapp/dictionary/diff/delete', { pair_key: pairKey });
      void loadHistory();
    } catch (e) {
      setPopular(before);
      setHistory(beforeHistory);
      setError(humanizeDictError(e));
    }
  }, [isAdmin, popular, history, loadHistory]);

  const answerChoice = useCallback((word, key) => {
    const next = { ...picksRef.current, [word]: key };
    picksRef.current = next;
    setPicks(next);
    haptic('light');
    const pending = choices.filter((row) => !next[row.word]);
    if (pending.length) {
      setChoices(pending);
      return;
    }
    setChoices([]);
    const words = normalizeCells(cells);
    if (words.length >= MIN_WORDS) void runDiff(words.slice(0, MAX_WORDS));
  }, [choices, cells, runDiff]);

  const startOver = useCallback(() => {
    setCells(['', '']);
    setResult(null);
    setMissing([]);
    setError('');
    setSaved(new Set());
    setChoices([]);
    setPicks({});
    picksRef.current = {};
    setPhase('idle');
  }, []);

  // Сохранение — оптимистичное: галочка сразу, работа в фоне. Не вышло — галочка
  // снимается, и человек видит человеческий текст, а не молчание.
  const saveOptimistic = useCallback(async (key, run) => {
    if (saved.has(key)) return;
    setSaved((prev) => new Set(prev).add(key));
    haptic('ok');
    try {
      await run();
    } catch (e) {
      setSaved((prev) => { const next = new Set(prev); next.delete(key); return next; });
      setError(humanizeDictError(e));
    }
  }, [saved]);

  const saveWord = useCallback((word) => {
    void saveOptimistic(`w:${word}`, () => saveLookedUpWord({
      api, text: word, origin: 'webapp_word_diff_word',
    }));
  }, [saveOptimistic]);

  const saveCollocation = useCallback((phrase, translation) => {
    void saveOptimistic(`c:${phrase}`, () => savePhraseWithTranslation({
      api, phrase, translation, origin: 'webapp_word_diff_collocation',
    }));
  }, [saveOptimistic]);

  const share = useCallback(async () => {
    const pairKey = result?.pair_key;
    if (!pairKey || sharing) return;
    setSharing(true);
    haptic('light');
    try {
      const data = await api('/api/webapp/dictionary/diff/share/link', { pair_key: pairKey });
      const link = String(data?.deeplink || '').trim();
      if (link && window.Telegram?.WebApp?.openTelegramLink) {
        const text = `Чем отличаются: ${(result?.words || []).join(' · ')}`;
        window.Telegram.WebApp.openTelegramLink(
          `https://t.me/share/url?url=${encodeURIComponent(link)}&text=${encodeURIComponent(text)}`,
        );
      }
    } catch (e) {
      setError(humanizeDictError(e));
    } finally {
      setSharing(false);
    }
  }, [result, sharing]);

  // ── Экран разбора ──────────────────────────────────────────────────────────
  if (phase === 'ready' && result?.diff) {
    const diff = result.diff || {};
    const words = Array.isArray(result.words) ? result.words : [];
    const comparable = diff.comparable || {};
    const noOverlap = comparable.value === 'none';
    const overlap = diff.overlap || {};
    const pairs = Array.isArray(diff.interchangeable) ? diff.interchangeable : [];
    const cards = Array.isArray(diff.words) ? diff.words : [];
    const examples = Array.isArray(diff.examples) ? diff.examples : [];
    const chooser = Array.isArray(diff.chooser) ? diff.chooser : [];
    const constructions = Array.isArray(diff.constructions) ? diff.constructions : [];
    const collocations = Array.isArray(diff.collocations) ? diff.collocations : [];
    const usage = Array.isArray(diff.usage) ? diff.usage : [];

    return (
      <div className="wd-root">
        {!isGuest && (
          <button type="button" className="wd-back" onClick={() => setPhase('idle')}>
            ← Назад к словам
          </button>
        )}

        {streaming && <div className="wd-streaming">Дописываю разбор…</div>}

        <div className="wd-pair">
          {words.map((word, i) => (
            <span key={word}>
              {i > 0 && <span className="wd-sep">·</span>}
              {word}
            </span>
          ))}
        </div>

        {/* 1. ГЛАВНОЕ — вердикт, пересечение и заменяемость одним блоком: раздельно они
            начинали пересказывать одну мысль четырьмя способами. */}
        <div className="wd-block">
          <div className="wd-label"><span className="wd-ic">💡</span>Главное</div>

          {noOverlap && comparable.note && (
            <div className="wd-compare-note is-none">
              <b>Это не синонимы.</b> {comparable.note}
            </div>
          )}
          {!noOverlap && comparable.value === 'partial' && comparable.note && (
            <div className="wd-compare-note is-partial">{comparable.note}</div>
          )}

          {Array.isArray(diff.verdict) && diff.verdict.length > 0 && (
            <div className="wd-verdict">
              {diff.verdict.map((row) => (
                <div className="wd-verdict-row" key={row.word}>
                  <b className="wd-em">{row.word}</b> — {emphasize(row.line, words)}
                </div>
              ))}
            </div>
          )}

          {overlap.note && <div className="wd-trap">{emphasize(overlap.note, words)}</div>}
          {(overlap.roles || []).length > 0 && (
            <div className="wd-when">
              {overlap.roles.map((row) => (
                <div className="wd-when-row" key={row.word}>
                  <span className="wd-when-word">{row.word}</span>
                  <span className="wd-when-sit">{emphasize(row.role, words)}</span>
                </div>
              ))}
            </div>
          )}

          {pairs.map((pair) => (
            <div className="wd-swap" key={`${pair.a}-${pair.b}`}>
              <span className={`wd-swap-tag is-${pair.value}`}>
                {INTERCHANGEABLE_LABEL[pair.value] || ''}
              </span>
              <span className="wd-swap-pair">{pair.a} ↔ {pair.b}</span>
              {pair.note && <span className="wd-swap-note">{emphasize(pair.note, words)}</span>}
            </div>
          ))}
        </div>

        {/* 2. СЛОВА ПО ОТДЕЛЬНОСТИ */}
        {cards.length > 0 && (
          <div className="wd-block">
            <div className="wd-label"><span className="wd-ic">📇</span>Слова по отдельности</div>
            {cards.map((card) => {
              const isSaved = saved.has(`w:${card.word}`);
              const mine = constructions.filter((c) => c.word === card.word);
              const senses = Array.isArray(card.senses) ? card.senses : [];
              // Конструкции без привязки к значению показываем один раз внизу карточки —
              // они относятся к слову целиком.
              const loose = mine.filter((c) => !c.sense_id
                || !senses.some((sense) => sense.sense_id === c.sense_id));
              return (
                <div className="wd-card" key={card.word}>
                  <div className="wd-card-top">
                    <span className="wd-card-word">{card.word}</span>
                    {tts && <SpeakButton text={card.word} tts={tts} />}
                    {card.pos && <span className="wd-pos">{POS_LABEL[card.pos] || card.pos}</span>}
                  </div>

                  {senses.map((sense, i) => {
                    const senseConstructions = mine.filter((c) => c.sense_id && c.sense_id === sense.sense_id);
                    return (
                      <div className="wd-sense" key={sense.sense_id || `s-${i}`}>
                        {senses.length > 1 && <span className="wd-sense-num">{i + 1}</span>}
                        <div className="wd-sense-body">
                          {sense.meaning && <div className="wd-card-gloss">{sense.meaning}</div>}
                          {sense.when && (
                            <div className="wd-card-row"><span className="k">Когда</span><span className="v">{emphasize(sense.when, words)}</span></div>
                          )}
                          {sense.register && (
                            <div className="wd-card-row"><span className="k">Регистр</span><span className="v">{sense.register}</span></div>
                          )}
                          {senseConstructions.length > 0 && (
                            <div className="wd-card-row">
                              <span className="k">Конструкция</span>
                              <span className="v">
                                {senseConstructions.map((c) => (
                                  <span className="wd-construction" key={c.id}>
                                    <b>{c.pattern}</b>
                                    {c.bare ? <span className="wd-bare"> — без дополнения</span> : null}
                                    {!c.bare && c.case ? ` · ${c.case}` : ''}
                                    {c.obligatory ? <span className="wd-oblig"> без предлога не употребляется</span> : null}
                                    {c.example_de && (
                                      <span className="wd-construction-ex">
                                        {c.example_de}
                                        {c.example_ru && <span className="wd-construction-ru">{c.example_ru}</span>}
                                      </span>
                                    )}
                                  </span>
                                ))}
                              </span>
                            </div>
                          )}
                        </div>
                      </div>
                    );
                  })}

                  {loose.length > 0 && (
                    <div className="wd-card-row">
                      <span className="k">Конструкция</span>
                      <span className="v">
                        {loose.map((c) => (
                          <span className="wd-construction" key={c.id}>
                            <b>{c.pattern}</b>
                            {c.bare ? <span className="wd-bare"> — без дополнения</span> : null}
                            {!c.bare && c.case ? ` · ${c.case}` : ''}
                            {c.obligatory ? <span className="wd-oblig"> без предлога не употребляется</span> : null}
                            {c.example_de && (
                              <span className="wd-construction-ex">
                                {c.example_de}
                                {c.example_ru && <span className="wd-construction-ru">{c.example_ru}</span>}
                              </span>
                            )}
                          </span>
                        ))}
                      </span>
                    </div>
                  )}

                  {!isGuest && (
                    <div className="wd-card-foot">
                      <button
                        type="button"
                        className={`wd-mini${isSaved ? ' is-saved' : ''}`}
                        onClick={() => saveWord(card.word)}
                        disabled={isSaved}
                      >
                        {isSaved ? '✓ Сохранено' : 'Сохранить'}
                      </button>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}

        {/* 3. ПРИМЕР С КОНТРАСТОМ — крест только там, где замена правда неверна */}
        {examples.length > 0 && (
          <div className="wd-block">
            <div className="wd-label"><span className="wd-ic">🔀</span>Одна ситуация — разные слова</div>
            {words.map((word) => {
              const mine = examples.filter((ex) => ex.word === word);
              if (!mine.length) return null;
              return (
                <div className="wd-ex-group" key={`ex-${word}`}>
                  <div className="wd-ex-group-title">{word}</div>
                  {mine.map((ex, i) => (
                    <div key={`${ex.de}-${i}`}>
                      <div className="wd-ex is-ok">
                        <span className="wd-ex-mark">✓</span>
                        <div className="wd-ex-body">
                          <div className="wd-ex-de">{emphasize(ex.de, words)}</div>
                          <div className="wd-ex-ru">{ex.translation}</div>
                        </div>
                      </div>
                      {ex.contrast && (
                        <div className={`wd-ex ${ex.contrast.type === 'wrong' ? 'is-no' : 'is-shift'}`}>
                          <span className="wd-ex-mark">{ex.contrast.type === 'wrong' ? '✗' : '≠'}</span>
                          <div className="wd-ex-body">
                            <div className="wd-ex-de">{emphasize(ex.contrast.de, words)}</div>
                            {ex.contrast.translation && <div className="wd-ex-ru">{ex.contrast.translation}</div>}
                            <div className="wd-ex-why">
                              {CONTRAST_LABEL[ex.contrast.type] || ''}
                              {ex.contrast.why ? ` ${ex.contrast.why}` : ''}
                            </div>
                          </div>
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              );
            })}
          </div>
        )}

        {/* 4. КОГДА КАКОЕ */}
        {chooser.length > 0 && (
          <div className="wd-block">
            <div className="wd-label"><span className="wd-ic">🧭</span>Когда какое</div>
            <div className="wd-when">
              {chooser.map((row, i) => (
                <div className="wd-when-row" key={`${row.word}-${i}`}>
                  <span className="wd-when-sit">{emphasize(row.situation, words)}</span>
                  <span className="wd-when-word">{row.word}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* 5. КАК СЛОВО УПОТРЕБЛЯЕТСЯ — возвратность и родня по приставкам */}
        {usage.some((u) => (u.reflexivity || {}).value || (u.word_family || []).length) && (
          <div className="wd-block">
            <div className="wd-label"><span className="wd-ic">🔗</span>Как слово употребляется</div>
            {usage.map((u) => {
              const refl = u.reflexivity || {};
              const forms = Array.isArray(refl.forms) ? refl.forms : [];
              const family = Array.isArray(u.word_family) ? u.word_family : [];
              if (!refl.value && !family.length) return null;
              return (
                <div className="wd-usage" key={`usage-${u.word}`}>
                  <div className="wd-ex-group-title">{u.word}</div>
                  {refl.value && (
                    <div className="wd-usage-row">
                      <span className="k">Возвратность</span>
                      <span className="v">
                        {REFLEXIVITY_LABEL[refl.value] || refl.value}
                        {refl.note ? ` — ${refl.note}` : ''}
                      </span>
                    </div>
                  )}
                  {forms.length > 0 && (
                    <div className="wd-colloc">
                      {forms.map((f, i) => {
                        const isSaved = saved.has(`c:${f.form}`);
                        return (
                          <button
                            type="button"
                            key={`${f.form}-${i}`}
                            className={`wd-colloc-chip${isSaved ? ' is-saved' : ''}`}
                            onClick={() => !isGuest && saveCollocation(f.form, '')}
                            disabled={isGuest || isSaved}
                          >
                            <span className="wd-colloc-de">{isSaved ? `✓ ${f.form}` : f.form}</span>
                          </button>
                        );
                      })}
                    </div>
                  )}
                  {family.length > 0 && (
                    <div className="wd-usage-row">
                      <span className="k">Родня</span>
                      <span className="v">
                        {family.map((row) => `${row.word} — ${row.meaning}`).join('; ')}
                      </span>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}

        {/* 6. ГДЕ СПОТЫКАЮТСЯ + ТИПИЧНЫЕ СОЧЕТАНИЯ */}
        {diff.trap && (
          <div className="wd-block">
            <div className="wd-label"><span className="wd-ic">⚠️</span>Где спотыкаются</div>
            <div className="wd-trap is-warn">{emphasize(diff.trap, words)}</div>
          </div>
        )}

        {collocations.length > 0 && (
          <div className="wd-block">
            <div className="wd-label"><span className="wd-ic">🧩</span>Типичные сочетания</div>
            {!isGuest && <div className="wd-hint-line">Нажмите на любое — сохраним сочетание целиком, экран не уйдёт.</div>}
            {words.map((word) => {
              const mine = collocations.filter((row) => row.word === word);
              if (!mine.length) return null;
              return (
                <div className="wd-colloc-group" key={`col-${word}`}>
                  <div className="wd-ex-group-title">{word}</div>
                  <div className="wd-colloc">
                    {mine.map((row, i) => {
                      const isSaved = saved.has(`c:${row.phrase}`);
                      return (
                        <button
                          type="button"
                          key={`${row.phrase}-${i}`}
                          className={`wd-colloc-chip${isSaved ? ' is-saved' : ''}`}
                          onClick={() => !isGuest && saveCollocation(row.phrase, row.translation)}
                          disabled={isGuest || isSaved}
                        >
                          <span className="wd-colloc-de">{isSaved ? `✓ ${row.phrase}` : row.phrase}</span>
                          {row.translation && <span className="wd-colloc-ru">{row.translation}</span>}
                        </button>
                      );
                    })}
                  </div>
                </div>
              );
            })}
          </div>
        )}

        {error && <div className="wd-error">{error}</div>}

        {!isGuest && (
          <div className="wd-actions">
            <button type="button" className="wd-action is-primary" onClick={share} disabled={sharing}>
              {sharing ? 'Готовлю ссылку…' : 'Поделиться'}
            </button>
            <button type="button" className="wd-action" onClick={startOver}>Новое сравнение</button>
          </div>
        )}

        <div className="wd-source-foot">
          Значения, управление и сочетания собраны по нашим словарным статьям.
          Род, формы и уровень слова берутся из справочника, а не из этого разбора.
        </div>
      </div>
    );
  }

  // ── Экран ввода (и всё, что случилось по дороге) ───────────────────────────
  return (
    <div className="wd-root">
      <div className="wd-title">Чем отличаются слова?</div>
      <div className="wd-sub">Впишите похожие слова — покажем разницу, примеры и когда какое брать.</div>

      <div className="wd-cells">
        {cells.map((value, index) => (
          <div className={`wd-cell${value ? ' is-filled' : ''}`} key={index}>
            <span className="wd-cell-num">{index + 1}</span>
            <input
              className="wd-cell-input"
              type="text"
              autoComplete="off"
              placeholder={index === 0 ? 'Первое слово…' : 'Следующее слово…'}
              value={value}
              onChange={(e) => setCell(index, e.target.value)}
              onKeyDown={(e) => { if (e.key === 'Enter' && canSubmit) submit(); }}
            />
            {value && (
              <button type="button" className="wd-cell-clear" aria-label="Очистить"
                      onClick={() => setCell(index, '')}>×</button>
            )}
          </div>
        ))}
      </div>

      {cells.length < MAX_WORDS && (
        <button type="button" className="wd-add" onClick={addCell}>＋ ещё слово</button>
      )}

      {/* Кнопка стоит у ВСЕХ, включая бесплатный тариф.

          Раньше вместо неё бесплатному показывали плашку «Свои сравнения — в полном
          доступе», и это ломало работу: сервер отдаёт бесплатному ГОТОВУЮ пару из общей
          полки бесплатно (backend_server.py, _word_diff_can_create → get_word_diff_card
          с any_version), а попросить её человеку было нечем — кнопки не было. Он мог
          только тыкать по списку внизу и не мог набрать свои два слова.
          Теперь решает сервер: пара есть — открывается сразу и даром; пары нет — приходит
          ответ paid_only, и мы показываем модальное окно (см. выше). ПРОВЕРЕНО 27.08.2026. */}
      <button
        type="button"
        className={`wd-cta${canSubmit ? '' : ' is-off'}`}
        onClick={submit}
        disabled={!canSubmit}
      >
        {phase === 'loading' ? 'Разбираю…' : 'Объяснить отличия'}
      </button>

      {phase === 'loading' && (
        <div className="wd-steps">
          <div className="wd-step is-now">
            <span className="m">•</span>
            {stageText || 'Ищем слова в словаре'}
          </div>
          <div className="wd-step is-wait">
            <span className="m">·</span>
            Незнакомое слово разбираем целиком — это дольше, но один раз
          </div>
        </div>
      )}

      {phase === 'choose' && choices.length > 0 && (
        <div className="wd-choose">
          <div className="wd-choose-title">Что вы имеете в виду?</div>
          {choices.map((row) => (
            <div className="wd-choose-word" key={`choose-${row.word}`}>
              <div className="wd-choose-word-name">{row.word}</div>
              <div className="wd-choose-options">
                {(row.options || []).map((option) => (
                  <button
                    type="button"
                    className="wd-choose-option"
                    key={option.key}
                    onClick={() => answerChoice(row.word, option.key)}
                  >
                    <span className="wd-choose-label">{option.label}</span>
                    <span className="wd-choose-pos">{option.pos_label}</span>
                    {option.hint && <span className="wd-choose-hint">{option.hint}</span>}
                  </button>
                ))}
              </div>
            </div>
          ))}
          <div className="wd-choose-foot">
            Слово читается по-разному, и решать за вас мы не станем — скажите, что имели в виду.
          </div>
        </div>
      )}

      {phase === 'missing' && missing.length > 0 && (
        <div className="wd-notice is-fix">
          <div className="wd-notice-title">
            {missing.length === 1
              ? `«${missing[0].word}» в словарях не нашли`
              : 'Некоторых слов в словарях не нашли'}
          </div>
          {missing.map((row) => (
            <div className="wd-notice-row" key={row.word}>
              {row.suggestion ? (
                <>
                  <span>Есть похожее: <b>{row.suggestion}</b></span>
                  <button type="button" className="wd-notice-btn is-primary"
                          onClick={() => applySuggestion(row.word, row.suggestion)}>
                    Исправить
                  </button>
                </>
              ) : (
                <span>«{row.word}» — такого слова мы не нашли. Проверьте написание.</span>
              )}
            </div>
          ))}
          <div className="wd-notice-foot">
            Незнакомые слова мы разбираем на месте. Это слово не собралось и в разборе —
            значит, дело в написании, а не в нашей базе.
          </div>
        </div>
      )}

      {phase === 'error' && error && (
        <div className={`wd-notice ${limitReached ? 'is-paid' : 'is-error'}`}>
          <div className="wd-notice-title">
            {limitReached ? 'Сегодня разобрали все бесплатные пары' : 'Не получилось разобрать'}
          </div>
          <div className="wd-notice-row"><span>{error}</span></div>
          {limitReached && onNeedFullAccess && (
            <div className="wd-notice-row">
              <button type="button" className="wd-notice-btn is-primary" onClick={onNeedFullAccess}>
                Открыть полный доступ
              </button>
            </div>
          )}
          {limitReached && history.length > 0 && (
            <div className="wd-notice-foot">Разобранное раньше открывается бесплатно — список ниже.</div>
          )}
        </div>
      )}

      {!isGuest && history.length > 0 && (
        <div className="wd-block">
          <button
            type="button"
            className={`wd-fold${openLists.mine ? ' is-open' : ''}`}
            onClick={() => setOpenLists((prev) => ({ ...prev, mine: !prev.mine }))}
          >
            <span className="wd-label"><span className="wd-ic">🕘</span>Вы уже сравнивали</span>
            <span className="wd-fold-count">{history.length}</span>
            <span className="wd-fold-arrow" aria-hidden="true">▾</span>
          </button>
          {openLists.mine && (
          <div className="wd-history">
            {history.map((row) => (
              <div className={`wd-swipe${swiped === row.pair_key ? ' is-open' : ''}`} key={row.pair_key}>
                <button
                  type="button"
                  className="wd-history-row"
                  onPointerDown={(e) => onRowPointerDown(e, row.pair_key)}
                  onPointerUp={(e) => onRowPointerUp(e, row.pair_key)}
                  onClick={() => (swiped === row.pair_key ? setSwiped('') : openPair(row.words))}
                >
                  <span className="wd-history-pair">{(row.words || []).join(' · ')}</span>
                  {row.opens > 0 && <span className="wd-history-count">{row.opens}</span>}
                </button>
                {isAdmin && (
                  <button
                    type="button"
                    className="wd-swipe-delete"
                    aria-label="Убрать разбор с общей полки"
                    onClick={() => removePair(row.pair_key)}
                  >
                    Удалить
                  </button>
                )}
              </div>
            ))}
          </div>
          )}
        </div>
      )}

      {/* Общий список: что разбирали ВСЕ. Открывается из базы, поэтому бесплатен и для
          бесплатного тарифа. Порядок по числу обращений — он же и защита от мусора:
          случайную ерунду никто не открывает второй раз, и она уходит вниз. */}
      {!isGuest && popular.length > 0 && (
        <div className="wd-block">
          <button
            type="button"
            className={`wd-fold${openLists.shelf ? ' is-open' : ''}`}
            onClick={() => setOpenLists((prev) => ({ ...prev, shelf: !prev.shelf }))}
          >
            <span className="wd-label"><span className="wd-ic">📚</span>Уже разобрано — открывается бесплатно</span>
            <span className="wd-fold-count">{popular.length}</span>
            <span className="wd-fold-arrow" aria-hidden="true">▾</span>
          </button>
          {openLists.shelf && (
          <div className="wd-history">
            {popular.map((row) => (
              <div className={`wd-swipe${swiped === row.pair_key ? ' is-open' : ''}`} key={`p-${row.pair_key}`}>
                <button
                  type="button"
                  className="wd-history-row"
                  onPointerDown={(e) => onRowPointerDown(e, row.pair_key)}
                  onPointerUp={(e) => onRowPointerUp(e, row.pair_key)}
                  onClick={() => (swiped === row.pair_key ? setSwiped('') : openPair(row.words))}
                >
                  <span className="wd-history-pair">{(row.words || []).join(' · ')}</span>
                  <span className="wd-history-count">{row.opens}</span>
                </button>
                {isAdmin && (
                  <button
                    type="button"
                    className="wd-swipe-delete"
                    aria-label="Убрать разбор с общей полки"
                    onClick={() => removePair(row.pair_key)}
                  >
                    Удалить
                  </button>
                )}
              </div>
            ))}
          </div>
          )}
        </div>
      )}

      {/* Окно «нужен полный доступ» — ОДНО на оба словаря, тот же вид, что и во всём
          приложении. Что делает его кнопка, решает тот, кто вставил компонент: во
          внутреннем словаре она открывает «Подписку», в быстром — бота в Telegram.
          Само окно наружу не уводит НИКОГДА: человек закрывает его и остаётся на месте.
          Текст здесь русский, как и весь этот экран. */}
      <ProFeatureModal
        isOpen={paywallOpen}
        onClose={() => setPaywallOpen(false)}
        onUpgrade={() => { setPaywallOpen(false); if (onNeedFullAccess) onNeedFullAccess(); }}
        tr={(ru) => ru}
        emoji="⚖️"
        title="Новое сравнение — в полном доступе"
        intro="Этой пары ещё никто не разбирал. Разбор мы делаем на месте — он открыт в полном доступе."
        bullets={[
          'Всё, что уже разобрано, открывается бесплатно — список на этом экране.',
          'С полным доступом сравниваете любые свои слова, без счётчика.',
        ]}
      />
    </div>
  );
}
