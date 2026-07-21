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

// Save a GERMAN word/phrase tapped inside a game or trainer.
//
// The word is persisted FIRST with the gloss the game already knows — one cheap call,
// no LLM. Only then does the full breakdown (article + Grundform + examples) get fetched
// and re-saved over the same entry, in the background.
//
// The order matters: the breakdown endpoint is billed and sits behind the daily cost cap,
// so a lookup-first pipeline silently dropped every tap once the cap was reached (429 →
// nothing saved at all, only an error haptic). Saving cannot depend on a paid request.
// A card that stays thin is not lost either — `bt_3_dictionary_entries` is a SHARED pool,
// so the nightly enricher or the next lookup of that word by anyone fills it in.
//
// `fallbackTranslation` is the gloss the game already knows. When it is present (the
// normal case — the Artikel/Sprint decks all carry a Russian meaning) the word goes in
// instantly and the breakdown follows. When the game knows NO translation there is
// nothing to store yet, so the breakdown has to come first — but a failed lookup still
// ends in a save, so a tap never silently loses the word.
//
// Resolves once the word is safely stored; rejects only if THAT fails.
export async function saveGermanWordViaLookup({ api, word, fallbackTranslation = '', origin }) {
  const text = String(word || '').trim();
  if (!text) return null;
  // Validate the script so a German string can never land in the Russian slot.
  const knownRu = pickTargetTranslation('ru', [fallbackTranslation]);
  if (!knownRu) {
    let item = null;
    let direction = '';
    try {
      const lookup = await api('/api/webapp/dictionary', { word: text });
      item = (lookup && lookup.item) || null;
      direction = String(lookup?.direction || '').toLowerCase();
    } catch (_e) {
      item = null; // cap reached / offline — save the bare word rather than lose it
    }
    return saveDictionaryCard({ api, word: text, item, direction, fallbackRu: '', origin });
  }
  await api('/api/webapp/dictionary/save', {
    source_text: text,
    target_text: knownRu,
    translation_ru: knownRu,
    source_lang: 'de',
    target_lang: 'ru',
    direction: 'de-ru',
    origin_process: origin,
  });
  // Background enrichment — best effort, never surfaced. A capped/offline user keeps the
  // word; the card fills in later.
  void enrichSavedGermanWord({ api, word: text, knownRu, origin });
  return { sourceText: text, targetText: knownRu };
}

// Persist a card built from a breakdown `item` (which may be null — then only the bare
// German text is stored). `pinnedSource`/`pinnedTarget` force the dedup keys the server
// matches on (see get_existing_user_dictionary_entry_id_for_save: source_text_norm +
// target_text_norm), so an enrichment pass UPDATES the row the thin save created instead
// of adding a second, near-identical entry.
async function saveDictionaryCard({
  api, word, item, direction, fallbackRu, origin, pinnedSource = '', pinnedTarget = '',
}) {
  const dir = String(direction || '').toLowerCase();
  const isDeRu = dir !== 'ru-de'; // game words are German → de-ru
  const targetLang = isDeRu ? 'ru' : 'de';
  const card = item && typeof item === 'object' ? item : {};
  const sourceText = pinnedSource || String(
    (isDeRu ? (card.word_de || card.translation_de) : (card.word_ru || card.translation_ru)) || word,
  ).trim();
  const targetText = pinnedTarget || pickTargetTranslation(targetLang, [
    isDeRu ? extractRichTranslation(card) : '',
    isDeRu ? card.translation_ru : card.translation_de,
    isDeRu ? card.word_ru : card.word_de,
    fallbackRu,
  ]);
  await api('/api/webapp/dictionary/save', {
    source_text: sourceText,
    target_text: targetText,
    translation_ru: String(isDeRu ? targetText : (card.translation_ru || '')).trim(),
    translation_de: String(isDeRu ? (card.translation_de || '') : targetText).trim(),
    source_lang: isDeRu ? 'de' : 'ru',
    target_lang: targetLang,
    direction: dir || 'de-ru',
    response_json: item || undefined,
    origin_process: origin,
  });
  return { sourceText, targetText };
}

// Fetch the full breakdown for an ALREADY-SAVED German word and re-save the complete card
// over the SAME entry. Split out of saveGermanWordViaLookup so the user's save never waits
// on — or dies with — this billed call.
async function enrichSavedGermanWord({ api, word, knownRu, origin }) {
  try {
    const lookup = await api('/api/webapp/dictionary', { word });
    const item = (lookup && lookup.item) || null;
    if (!item || !Object.keys(item).length) return;
    await saveDictionaryCard({
      api, word, item, direction: lookup?.direction, fallbackRu: knownRu, origin,
      // Keep the thin save's keys so this lands on that entry, not next to it.
      pinnedSource: word, pinnedTarget: knownRu,
    });
  } catch (_e) {
    // Cap reached / offline / lookup failed — the word is already saved, so this is a
    // no-op by design. The shared pool fills the card in later.
  }
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
