/**
 * Shared dictionary save helpers — used by BOTH the quick-dictionary overlay and the
 * full "Полный разбор" (DeepAnalysis) card so every save source (typed word, tapped
 * synonym/related chip, save-worthy option) funnels through ONE canonical pipeline and
 * always lands in the same standard card format (article+Grundform / translation /
 * grammar), never bare German text without a translation.
 */

// Cheap language guess for the instant path: any Cyrillic → translate ru→de,
// otherwise treat as German → ru. The full GPT lookup auto-detects properly.
export function guessPair(text) {
  const hasCyrillic = /[А-Яа-яЁё]/.test(String(text || ''));
  return hasCyrillic
    ? { source: 'ru', target: 'de' }
    : { source: 'de', target: 'ru' };
}

// The GPT breakdown carries the target-language meaning in `translations`
// (array of {value, context} or strings) / `meanings.primary` — NOT in
// word_ru/translation_ru. Pull a concise translation string out of it so a save
// made straight from the deep card (without a fresh quick-translate) still keeps
// the translation instead of falling back to the German word. Top 2 meanings.
export function extractRichTranslation(item) {
  if (!item || typeof item !== 'object') return '';
  const out = [];
  const push = (v) => {
    const s = String((v && typeof v === 'object') ? v.value : v || '').trim();
    if (s && !out.includes(s)) out.push(s);
  };
  if (Array.isArray(item.translations)) {
    for (const t of item.translations) {
      if (out.length >= 2) break;
      push(t);
    }
  }
  if (!out.length && item.meanings && typeof item.meanings === 'object') {
    push(item.meanings.primary);
  }
  return out.join(', ');
}

// Pick the translation that actually belongs to `targetLang` from a list of candidates.
// For a Russian target we REQUIRE Cyrillic, so a German value leaking into the GPT
// breakdown's translation column can never be saved as the "Russian" translation — the
// recurring bug where a tapped synonym/antonym chip saved a German-only card. Falls back
// to the first non-empty candidate only if none match the expected script.
export function pickTargetTranslation(targetLang, candidates) {
  const list = candidates.map((c) => String(c || '').trim()).filter(Boolean);
  if (!list.length) return '';
  const hasCyrillic = (s) => /[А-Яа-яЁё]/.test(s);
  if (targetLang === 'ru') return list.find(hasCyrillic) || list[0];
  if (targetLang === 'de') return list.find((s) => !hasCyrillic(s)) || list[0];
  return list[0];
}

// Save a GERMAN word/phrase tapped inside a game or trainer through the SAME canonical
// pipeline the dictionary overlay uses: first the breakdown lookup (real Russian
// translation + article + Grundform + examples), then persist THAT card. Posting the
// German text straight to /save — the old behaviour of the grammar trainers — produced
// entries with German on both sides (translation == headword) and, because they carried
// no response_json, the server-side enrichment never ran either, so the card stayed empty.
// `fallbackTranslation` is the gloss the game already knows (may be empty); it is only
// used when the lookup returns nothing usable.
export async function saveGermanWordViaLookup({ api, word, fallbackTranslation = '', origin }) {
  const text = String(word || '').trim();
  if (!text) return null;
  const lookup = await api('/api/webapp/dictionary', { word: text });
  const item = (lookup && lookup.item) || {};
  const direction = String(lookup?.direction || '').toLowerCase();
  const isDeRu = direction !== 'ru-de'; // game words are German → de-ru
  const targetLang = isDeRu ? 'ru' : 'de';
  const sourceText = String(
    (isDeRu ? (item.word_de || item.translation_de) : (item.word_ru || item.translation_ru)) || text,
  ).trim();
  // Validate the script so a German string can never land in the Russian slot.
  const targetText = pickTargetTranslation(targetLang, [
    isDeRu ? extractRichTranslation(item) : '',
    isDeRu ? item.translation_ru : item.translation_de,
    isDeRu ? item.word_ru : item.word_de,
    fallbackTranslation,
  ]);
  await api('/api/webapp/dictionary/save', {
    source_text: sourceText,
    target_text: targetText,
    translation_ru: String(isDeRu ? targetText : (item.translation_ru || '')).trim(),
    translation_de: String(isDeRu ? (item.translation_de || '') : targetText).trim(),
    source_lang: isDeRu ? 'de' : 'ru',
    target_lang: targetLang,
    direction: direction || 'de-ru',
    response_json: item,
    origin_process: origin,
  });
  return { sourceText, targetText };
}

// Build the canonical /api/webapp/dictionary/save payload from a GPT breakdown item.
// EVERY save source (typed word OR tapped synonym/related chip) goes through this so
// the entry always lands in the same standard card format (article+Grundform /
// translation / grammar), never bare German text without a translation.
export function buildDictionarySavePayload({ rich, sourceText, quick, origin }) {
  const quickTranslation = quick?.translation || '';
  const quickDirection = quick?.direction || '';
  const quickSourceLang = quick?.sourceLang || '';
  const quickTargetLang = quick?.targetLang || '';
  const direction = String(rich?.__direction || quickDirection || '').trim();
  const [dirSource, dirTarget] = direction.includes('-') ? direction.split('-', 2) : [];
  const sourceLang = (dirSource || quickSourceLang || '').toLowerCase();
  const targetLang = (dirTarget || quickTargetLang || '').toLowerCase();
  const richTranslation = extractRichTranslation(rich);
  // The deterministic quick translate is the most reliable target-language gloss; the
  // GPT breakdown's extracted meaning and stored columns are fallbacks. Validate the
  // script (see pickTargetTranslation) so de↔ru pollution never lands in the wrong slot.
  const targetText = pickTargetTranslation(targetLang, [
    quickTranslation, richTranslation, rich?.translation_ru, rich?.translation_de,
  ]);
  return {
    word_de: String(rich?.word_de || '').trim(),
    word_ru: String(rich?.word_ru || '').trim(),
    translation_de: (targetLang === 'de' ? targetText : String(rich?.translation_de || '')).trim(),
    translation_ru: (targetLang === 'ru' ? targetText : String(rich?.translation_ru || '')).trim(),
    source_text: sourceText,
    target_text: targetText,
    source_lang: sourceLang || undefined,
    target_lang: targetLang || undefined,
    direction: direction || undefined,
    response_json: rich || undefined,
    origin_process: origin,
  };
}
