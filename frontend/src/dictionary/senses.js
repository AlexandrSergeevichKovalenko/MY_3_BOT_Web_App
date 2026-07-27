// Разрезание «свалки» перевода на отдельные значения — для ПОКАЗА.
//
// В личных карточках перевод хранится строкой, и у части слов это склейка смыслов:
// «1прикладывать; накладывать, приставлять 2 надевать 3 строить, закладывать…».
// В тренировке такая карточка неотвечаема: человек знал «надевать», а на карточке ещё
// пять смыслов — честно нажать «знаю» нельзя, а «не знаю» гоняет по кругу известное.
//
// Здесь мы НИЧЕГО не меняем в данных: только показываем первое значение крупно, а
// остальные — отдельной строкой. Правила разреза те же, что на сервере
// (scripts/dict_units_split_senses.py), чтобы словарь и тренировка не расходились.

// Номер значения: «1прикладывать», «2 надевать», «3. строить».
const SENSE_NUM_RE = /(?:^|[\s;,])(\d{1,2})\s*[).]?\s*(?=[А-Яа-яЁёA-Za-z])/g;
// Пометки в скобках: «(перен.)», «(разг.)», «(куда-либо)».
const PAREN_RE = /\s*\(([^)]{1,60})\)\s*/g;
// Словарные пометки в начале значения: «vi причаливать», «vt. ставить».
const MARKER_RE = /^((?:v[itrp]|vimp|refl|adj|adv|разг|перен|уст)\.?)\s+/i;
// Грамматическая помета — это не перевод, а служебная запись.
const GRAMMAR_NOTE_RE = /\b(genitiv|dativ|akkusativ|nominativ|plural|singular|мн\.?\s*ч|ед\.?\s*ч)\b/i;

const clean = (value) => String(value == null ? '' : value).replace(/\s+/g, ' ').trim();

/**
 * Строка перевода → список значений [{ value, label }].
 * Если резать нечего, возвращается один элемент с исходным текстом.
 *
 * Запятую НЕ трогаем: «строить, закладывать, сооружать» — оттенки одного значения,
 * дробить их на отдельные строки было бы мельче, чем нужно человеку.
 */
export function splitTranslationSenses(text) {
  const raw = clean(text);
  if (!raw) return [];

  const marks = [];
  SENSE_NUM_RE.lastIndex = 0;
  let match = SENSE_NUM_RE.exec(raw);
  while (match) {
    marks.push({ numStart: match.index + match[0].indexOf(match[1]), bodyStart: SENSE_NUM_RE.lastIndex });
    match = SENSE_NUM_RE.exec(raw);
  }

  let chunks;
  if (marks.length >= 2) {
    chunks = marks.map((mark, index) => raw.slice(
      mark.bodyStart,
      index + 1 < marks.length ? marks[index + 1].numStart : raw.length,
    ));
    const head = raw.slice(0, marks[0].numStart).replace(/[\s;,]+$/, '').trim();
    if (head) chunks.unshift(head);
  } else {
    chunks = [raw];
  }

  const out = [];
  const seen = new Set();
  chunks.forEach((chunk) => {
    chunk.split(/\s*;\s*/).forEach((piece) => {
      const labels = [];
      let value = clean(piece.replace(PAREN_RE, (_m, inner) => {
        labels.push(clean(inner));
        return ' ';
      })).replace(/^[\s,;.—-]+|[\s,;.—-]+$/g, '');
      const marker = value.match(MARKER_RE);
      if (marker) {
        labels.push(marker[1]);
        value = value.slice(marker[0].length).replace(/^[\s,;.—-]+/, '');
      }
      value = value.replace(/\s+([,;])/g, '$1');
      if (!value || value.length < 2) return;
      if (!/[А-Яа-яЁёA-Za-z]/.test(value)) return;
      if (GRAMMAR_NOTE_RE.test(value)) return;
      const key = value.toLowerCase();
      if (seen.has(key)) return;
      seen.add(key);
      out.push({ value, label: labels.filter((l) => l && !GRAMMAR_NOTE_RE.test(l)).join('; ') });
    });
  });

  return out.length ? out : [{ value: raw, label: '' }];
}

export default splitTranslationSenses;
