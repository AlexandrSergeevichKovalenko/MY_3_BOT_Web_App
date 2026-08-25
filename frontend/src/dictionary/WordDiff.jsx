import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { api, haptic, SpeakButton, getInitData, getDictToken } from './WordBreakdown';
import { humanizeDictError } from './errors.js';
import { saveLookedUpWord, savePhraseWithTranslation } from './saveUtils';

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

const HINTS = [
  ['Anzahlung', 'Vorauszahlung'],
  ['kennen', 'wissen'],
  ['machen', 'tun'],
];

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
  const [history, setHistory] = useState([]);
  const [saved, setSaved] = useState(() => new Set());
  const [sharing, setSharing] = useState(false);
  const [streaming, setStreaming] = useState(false); // разбор ещё дописывается

  const filled = useMemo(() => normalizeCells(cells), [cells]);
  const canSubmit = filled.length >= MIN_WORDS && phase !== 'loading';
  const isGuest = Boolean(sharedToken);

  const loadHistory = useCallback(async () => {
    try {
      const data = await api('/api/webapp/dictionary/diff/history', { limit: 20 });
      setHistory(Array.isArray(data?.items) ? data.items : []);
    } catch (_e) {
      // История — витрина, а не ответ на вопрос человека. Её отсутствие не должно
      // мешать сравнивать слова, поэтому здесь тихо. Ошибка самого разбора — громкая.
      setHistory([]);
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
  const runDiff = useCallback(async (words) => {
    setPhase('loading');
    setError('');
    setLimitReached(false);
    setMissing([]);
    setSaved(new Set());
    setStreaming(false);
    try {
      const token = getDictToken();
      const headers = { 'Content-Type': 'application/json', 'X-Telegram-InitData': getInitData() };
      if (token) headers['X-Dict-Token'] = token;
      const resp = await fetch('/api/webapp/dictionary/diff/stream', {
        method: 'POST',
        headers,
        body: JSON.stringify({ initData: getInitData(), ...(token ? { dqt: token } : {}), words }),
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
        if (event === 'section') {
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

  const applyHint = useCallback((pair) => {
    setCells(pair.slice(0, MAX_WORDS));
    haptic('light');
  }, []);

  const applySuggestion = useCallback((wrong, right) => {
    setCells((prev) => prev.map((t) => (String(t || '').trim() === wrong ? right : t)));
    setMissing([]);
    setPhase('idle');
  }, []);

  const startOver = useCallback(() => {
    setCells(['', '']);
    setResult(null);
    setMissing([]);
    setError('');
    setSaved(new Set());
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
    const inter = diff.interchangeable || {};
    const comparable = diff.comparable || {};
    const overlap = diff.overlap || {};
    const cards = Array.isArray(diff.words) ? diff.words : [];
    const examples = Array.isArray(diff.examples) ? diff.examples : [];
    const chooser = Array.isArray(diff.chooser) ? diff.chooser : [];
    const collocations = Array.isArray(diff.collocations) ? diff.collocations : [];

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

        {Array.isArray(diff.verdict) && diff.verdict.length > 0 && (
          <div className="wd-block">
            <div className="wd-label">Главное</div>
            <div className="wd-verdict">
              {diff.verdict.map((row) => (
                <div className="wd-verdict-row" key={row.word}>
                  <b>{row.word}</b> — {row.line}
                </div>
              ))}
            </div>
          </div>
        )}

        {(comparable.value === 'none' || comparable.value === 'one_sense') && comparable.note && (
          <div className="wd-block">
            <div className={`wd-compare-note is-${comparable.value}`}>
              <b>{COMPARABLE_LABEL[comparable.value]}.</b> {comparable.note}
            </div>
          </div>
        )}

        {(overlap.note || (overlap.roles || []).length > 0) && (
          <div className="wd-block">
            <div className="wd-label">Где значения пересекаются</div>
            {overlap.note && <div className="wd-trap">{overlap.note}</div>}
            {(overlap.roles || []).length > 0 && (
              <div className="wd-when">
                {overlap.roles.map((row) => (
                  <div className="wd-when-row" key={row.word}>
                    <span className="wd-when-word">{row.word}</span>
                    <span className="wd-when-sit">{row.role}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {inter.value && (
          <div className="wd-block">
            <div className="wd-swap">
              <span className={`wd-swap-tag is-${inter.value}`}>
                {INTERCHANGEABLE_LABEL[inter.value] || ''}
              </span>
              {inter.note && <span className="wd-swap-note">{inter.note}</span>}
            </div>
          </div>
        )}

        {cards.length > 0 && (
          <div className="wd-block">
            <div className="wd-label">Слова по отдельности</div>
            {cards.map((card) => {
              const isSaved = saved.has(`w:${card.word}`);
              return (
                <div className="wd-card" key={card.word}>
                  <div className="wd-card-top">
                    <span className="wd-card-word">{card.word}</span>
                    {tts && <SpeakButton text={card.word} tts={tts} />}
                  </div>
                  {card.meaning && <div className="wd-card-gloss">{card.meaning}</div>}
                  <div className="wd-card-rows">
                    {card.pos && (
                      <div className="wd-card-row"><span className="k">Часть речи</span><span className="v">{POS_LABEL[card.pos] || card.pos}</span></div>
                    )}
                    {(card.constructions || []).length > 0 && (
                      <div className="wd-card-row">
                        <span className="k">Конструкция</span>
                        <span className="v">
                          {card.constructions.map((c, i) => (
                            <span className="wd-construction" key={`${c.pattern}-${i}`}>
                              <b>{c.pattern}</b>{c.case ? ` · ${c.case}` : ''}
                              {c.example_de && <span className="wd-construction-ex">{c.example_de}</span>}
                            </span>
                          ))}
                        </span>
                      </div>
                    )}
                    {card.register && (
                      <div className="wd-card-row"><span className="k">Регистр</span><span className="v">{card.register}</span></div>
                    )}
                    {card.when && (
                      <div className="wd-card-row"><span className="k">Когда</span><span className="v">{card.when}</span></div>
                    )}
                    {card.where && (
                      <div className="wd-card-row"><span className="k">Где звучит</span><span className="v">{card.where}</span></div>
                    )}
                    {Array.isArray(card.partners) && card.partners.length > 0 && (
                      <div className="wd-card-row"><span className="k">С чем ходит</span><span className="v">{card.partners.join(', ')}</span></div>
                    )}
                  </div>
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

        {examples.length > 0 && (
          <div className="wd-block">
            <div className="wd-label">Одна ситуация — разные слова</div>
            {words.map((word) => {
              const mine = examples.filter((ex) => ex.word === word);
              if (!mine.length) return null;
              return (
                <div className="wd-ex-group" key={`ex-${word}`}>
                  <div className="wd-ex-group-title">{word}</div>
                  {mine.map((ex, i) => (
                    <div className={`wd-ex${ex.correct ? ' is-ok' : ' is-no'}`} key={`${ex.de}-${i}`}>
                      <span className="wd-ex-mark">{ex.correct ? '✓' : '✗'}</span>
                      <div className="wd-ex-body">
                        <div className="wd-ex-de">{ex.de}</div>
                        {ex.translation && <div className="wd-ex-ru">{ex.translation}</div>}
                        {ex.why && <div className="wd-ex-why">{ex.why}</div>}
                      </div>
                    </div>
                  ))}
                </div>
              );
            })}
            {examples.filter((ex) => !words.includes(ex.word)).map((ex, i) => (
              <div className={`wd-ex${ex.correct ? ' is-ok' : ' is-no'}`} key={`${ex.de}-${i}`}>
                <span className="wd-ex-mark">{ex.correct ? '✓' : '✗'}</span>
                <div className="wd-ex-body">
                  <div className="wd-ex-de">{ex.de}</div>
                  {ex.translation && <div className="wd-ex-ru">{ex.translation}</div>}
                  {ex.why && <div className="wd-ex-why">{ex.why}</div>}
                </div>
              </div>
            ))}
          </div>
        )}

        {chooser.length > 0 && (
          <div className="wd-block">
            <div className="wd-label">Когда какое</div>
            <div className="wd-when">
              {chooser.map((row, i) => (
                <div className="wd-when-row" key={`${row.word}-${i}`}>
                  <span className="wd-when-sit">{row.situation}</span>
                  <span className="wd-when-word">{row.word}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {diff.trap && (
          <div className="wd-block">
            <div className="wd-label">Где спотыкаются</div>
            <div className="wd-trap">{diff.trap}</div>
          </div>
        )}

        {collocations.length > 0 && (
          <div className="wd-block">
            <div className="wd-label">Типичные сочетания</div>
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
            <div className="wd-colloc">
              {collocations.filter((row) => !words.includes(row.word)).map((row, i) => {
                const isSaved = saved.has(`c:${row.phrase}`);
                return (
                  <button
                    type="button"
                    key={`${row.phrase}-${i}`}
                    className={`wd-colloc-chip${isSaved ? ' is-saved' : ''}`}
                    onClick={() => !isGuest && saveCollocation(row.phrase, row.translation)}
                    disabled={isGuest || isSaved}
                    title={row.translation || ''}
                  >
                    {isSaved ? `✓ ${row.phrase}` : row.phrase}
                  </button>
                );
              })}
            </div>
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
          Значения и примеры собраны по нашим словарным статьям и Wiktionary.
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

      {phase === 'idle' && filled.length === 0 && (
        <div className="wd-hints">
          {HINTS.map((pair) => (
            <button type="button" className="wd-hint-chip" key={pair.join('-')}
                    onClick={() => applyHint(pair)}>
              {pair.join(' · ')}
            </button>
          ))}
        </div>
      )}

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
          <div className="wd-step is-done"><span className="m">✓</span>Ищем слова в словаре</div>
          <div className="wd-step is-now"><span className="m">•</span>Сравниваем значения</div>
          <div className="wd-step is-wait"><span className="m">3</span>Подбираем примеры</div>
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
          <div className="wd-label">Вы уже сравнивали</div>
          <div className="wd-history">
            {history.map((row) => (
              <button
                type="button"
                className="wd-history-row"
                key={row.pair_key}
                onClick={() => {
                  const words = Array.isArray(row.words) ? row.words : [];
                  if (words.length < MIN_WORDS) return;
                  setCells(words.slice(0, MAX_WORDS));
                  void runDiff(words.slice(0, MAX_WORDS));
                }}
              >
                {(row.words || []).join(' · ')}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
