/**
 * Резка текста на предложения и слова — ОДНА на всё приложение.
 *
 * Зачем вынесено (10.09.2026). Механика «тап по слову — перевод, двойной тап —
 * предложение, удержание с протяжкой — фраза» написана в App.jsx трижды: читалка,
 * субтитры, разбор. Владелец попросил ту же механику в тренажёре синонимов, а
 * интерактивы (frontend/src/answer/*) монтируются ОТДЕЛЬНЫМ react-рутом
 * (main.jsx bootstrapAnswerOverlay) — App.jsx в их бандл не попадает вовсе.
 *
 * Вынесено ровно то, что не зависит ни от какого состояния: сама резка текста и
 * сборка выделенного куска. Жесты и плашка перевода у каждой поверхности свои
 * (у читалки к ним примешано аудио и листалка страниц), их не трогаем.
 *
 * ВАЖНО: идентификаторы предложений (sid) и слов (wid) считаются от ТЕКСТА, а не от
 * времени рендера, — на них завязаны data-атрибуты в DOM и поиск слова под пальцем.
 * Менять формат нельзя, не поправив читалку (ReaderSection.jsx ищет [data-wid=…]).
 */

function normalizeLangCode(value) {
  return String(value || '').trim().toLowerCase();
}

/** Схлопнуть пробелы: выделенный кусок текста показываем и переводим одной строкой. */
export function normalizeSelectionText(value) {
  if (!value) return '';
  return value.replace(/\s+/g, ' ').trim();
}

export function splitNonWordToken(value) {
  const chunks = [];
  if (!value) return chunks;
  const regex = /(\s+|[^\s]+)/g;
  let match = regex.exec(value);
  while (match) {
    const piece = String(match[0] || '');
    if (piece) {
      chunks.push({
        kind: /^\s+$/u.test(piece) ? 'space' : 'punct',
        value: piece,
        relativeStart: Number(match.index || 0),
        relativeEnd: Number(match.index || 0) + piece.length,
      });
    }
    match = regex.exec(value);
  }
  return chunks;
}

/**
 * Текст → [{sid, text, start, end, tokens:[{kind:'word'|'space'|'punct', wid, value, start, end}]}].
 * Intl.Segmenter, если он есть; иначе регулярки — на старом WebView без него страница
 * обязана остаться рабочей, а не пустой.
 */
export function segmentText(rawText, langHint) {
  const text = String(rawText || '');
  if (!text) return [];
  const safeLang = normalizeLangCode(langHint || '') || 'de';
  const wordRegex = /[A-Za-z0-9À-ÿА-Яа-яЁё''-]/u;

  const tokenizeSentence = (sentenceText, sentenceStart, sid) => {
    const tokens = [];
    let wordIndex = 0;
    if (typeof Intl !== 'undefined' && typeof Intl.Segmenter === 'function') {
      try {
        const wordSegmenter = new Intl.Segmenter(safeLang, { granularity: 'word' });
        const segmented = wordSegmenter.segment(sentenceText);
        for (const part of segmented) {
          const value = String(part?.segment || '');
          if (!value) continue;
          const tokenStart = Number(sentenceStart) + Number(part?.index || 0);
          const tokenEnd = tokenStart + value.length;
          if (part?.isWordLike) {
            const wid = `${sid}-w-${wordIndex}-${tokenStart}-${tokenEnd}`;
            wordIndex += 1;
            tokens.push({ kind: 'word', wid, value, start: tokenStart, end: tokenEnd });
            continue;
          }
          const chunks = splitNonWordToken(value);
          if (!chunks.length) {
            tokens.push({
              kind: /^\s+$/u.test(value) ? 'space' : 'punct',
              value,
              start: tokenStart,
              end: tokenEnd,
            });
            continue;
          }
          chunks.forEach((chunk) => {
            tokens.push({
              kind: chunk.kind,
              value: chunk.value,
              start: tokenStart + chunk.relativeStart,
              end: tokenStart + chunk.relativeEnd,
            });
          });
        }
        return tokens;
      } catch (_intlWordError) {
        // fallback below
      }
    }

    const fallbackWordTokenRegex = /(\s+|[A-Za-z0-9À-ÿА-Яа-яЁё''-]+|[^A-Za-z0-9À-ÿА-Яа-яЁё''-\s]+)/g;
    let match = fallbackWordTokenRegex.exec(sentenceText);
    while (match) {
      const value = String(match[0] || '');
      const tokenStart = Number(sentenceStart) + Number(match.index || 0);
      const tokenEnd = tokenStart + value.length;
      const isWord = wordRegex.test(value);
      if (isWord && !/^\s+$/u.test(value)) {
        const wid = `${sid}-w-${wordIndex}-${tokenStart}-${tokenEnd}`;
        wordIndex += 1;
        tokens.push({ kind: 'word', wid, value, start: tokenStart, end: tokenEnd });
      } else {
        tokens.push({
          kind: /^\s+$/u.test(value) ? 'space' : 'punct',
          value,
          start: tokenStart,
          end: tokenEnd,
        });
      }
      match = fallbackWordTokenRegex.exec(sentenceText);
    }
    return tokens;
  };

  const buildSentence = (sentenceText, startIndex, endIndex, sidIndex) => {
    const sid = `s-${sidIndex}-${startIndex}-${endIndex}`;
    return {
      sid,
      text: sentenceText,
      start: startIndex,
      end: endIndex,
      tokens: tokenizeSentence(sentenceText, startIndex, sid),
    };
  };

  const sentences = [];
  if (typeof Intl !== 'undefined' && typeof Intl.Segmenter === 'function') {
    try {
      const sentenceSegmenter = new Intl.Segmenter(safeLang, { granularity: 'sentence' });
      const segmented = sentenceSegmenter.segment(text);
      let sidIndex = 0;
      for (const part of segmented) {
        const sentenceText = String(part?.segment || '');
        if (!sentenceText) continue;
        const startIndex = Number(part?.index || 0);
        const endIndex = startIndex + sentenceText.length;
        sentences.push(buildSentence(sentenceText, startIndex, endIndex, sidIndex));
        sidIndex += 1;
      }
      if (sentences.length > 0) return sentences;
    } catch (_intlSentenceError) {
      // fallback below
    }
  }

  const fallbackRegex = /([.!?]+|\n+)/g;
  let sidIndex = 0;
  let cursor = 0;
  let match = fallbackRegex.exec(text);
  while (match) {
    const end = Number(match.index || 0) + String(match[0] || '').length;
    const segment = text.slice(cursor, end);
    if (segment) {
      sentences.push(buildSentence(segment, cursor, end, sidIndex));
      sidIndex += 1;
    }
    cursor = end;
    match = fallbackRegex.exec(text);
  }
  if (cursor < text.length) {
    const segment = text.slice(cursor);
    const end = text.length;
    if (segment) {
      sentences.push(buildSentence(segment, cursor, end, sidIndex));
    }
  }
  return sentences;
}

/** Слова предложения (без пробелов и знаков) — по ним считается номер слова в жесте. */
export function sentenceWords(sentence) {
  if (!sentence || !Array.isArray(sentence.tokens)) return [];
  return sentence.tokens.filter((token) => token.kind === 'word' && token.wid);
}

export function wordIndexInSentence(sentence, wordId) {
  return sentenceWords(sentence).findIndex((token) => String(token.wid || '') === String(wordId || ''));
}

/**
 * Кусок предложения от слова к слову → {text, sids, wids, start, end}.
 * Тянуть можно только ВНУТРИ одного предложения: перевод куска из двух разных
 * предложений смысла не имеет (то же правило, что в читалке).
 */
export function buildPhraseSelection(sentence, firstIndex, lastIndex) {
  if (!sentence) return null;
  const words = sentenceWords(sentence);
  if (!words.length) return null;
  const from = Math.max(0, Math.min(words.length - 1, Math.min(firstIndex, lastIndex)));
  const to = Math.max(0, Math.min(words.length - 1, Math.max(firstIndex, lastIndex)));
  const selectedWords = words.slice(from, to + 1);
  if (!selectedWords.length) return null;
  const start = Number(selectedWords[0]?.start || 0);
  const end = Number(selectedWords[selectedWords.length - 1]?.end || start);
  const relativeStart = Math.max(0, start - Number(sentence.start || 0));
  const relativeEnd = Math.max(relativeStart, end - Number(sentence.start || 0));
  const text = normalizeSelectionText(String(sentence.text || '').slice(relativeStart, relativeEnd));
  if (!text) return null;
  return {
    text,
    sids: [sentence.sid],
    wids: selectedWords.map((token) => String(token.wid || '')).filter(Boolean),
    start,
    end,
  };
}

/** Слово под пальцем: DOM-узел с data-wid/data-sid в точке экрана. */
export function getWordElementByPoint(clientX, clientY) {
  if (!Number.isFinite(clientX) || !Number.isFinite(clientY) || typeof document === 'undefined') return null;
  const target = document.elementFromPoint(clientX, clientY);
  if (!(target instanceof Element)) return null;
  return target.closest('[data-wid][data-sid]');
}
