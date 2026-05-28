/**
 * Frontend feature registry — explicit FREE_CORE vs HEAVY_PREMIUM boundary.
 *
 * FREE_CORE: always initialized, always available to every user.
 * HEAVY_PREMIUM: must NOT initialize before entitlement is resolved,
 *   and must NOT load at all for free users.
 *
 * Rule: anything in HEAVY_PREMIUM must be lazy-imported and guarded
 * with an entitlement check before initialization.
 */

export const FREE_CORE_FEATURES = Object.freeze({
  translations: 'translations',
  dictionary: 'dictionary',
  shortcut_ingest: 'shortcut_ingest',
  fsrs: 'fsrs',
  quizzes: 'quizzes',
  history: 'history',
  auth: 'auth',
  session: 'session',
});

export const HEAVY_PREMIUM_FEATURES = Object.freeze({
  skills: 'skills',
  today_plan: 'today_plan',
  weekly_plan: 'weekly_plan',
  analytics: 'analytics',
  youtube: 'youtube',
  livekit_assistant: 'livekit_assistant',
  advanced_ai_planning: 'advanced_ai_planning',
  reader_audio: 'reader_audio',
  economics: 'economics',
});

export const FREE_CORE_ROUTES = Object.freeze(new Set([
  'translations',
  'dictionary',
  'flashcards',
  'subscription',
  'guide',
  'support',
]));

export const HEAVY_PREMIUM_ROUTES = Object.freeze(new Set([
  'today',
  'home_today',
  'home_skills',
  'home_weekly_plan',
  'analytics',
  'youtube',
  'assistant',
  'economics',
]));

export const HEAVY_PREMIUM_MODULES = Object.freeze([
  { name: 'livekit', chunkHint: 'livekit-vendor', requiredModes: ['pro', 'trial'] },
  { name: 'echarts', chunkHint: 'charts-vendor', requiredModes: ['pro', 'trial', 'free'] },
]);

/**
 * Returns true when the resolved billing mode permits heavy module initialization.
 * 'free' → only FREE_CORE modules permitted.
 * 'trial' | 'pro' → HEAVY_PREMIUM modules permitted.
 * null / '' → entitlement not yet known → treat as free (conservative).
 */
export function isHeavyModulePermitted(effectiveMode, moduleName) {
  const mode = String(effectiveMode || '').trim().toLowerCase();
  const module = HEAVY_PREMIUM_MODULES.find((m) => m.name === moduleName);
  if (!module) return false;
  return module.requiredModes.includes(mode);
}

/**
 * Returns true when the effective mode is 'free' or unknown.
 * Used to gate heavy module initialization.
 */
export function isFreeCoreOnlyMode(effectiveMode) {
  const mode = String(effectiveMode || '').trim().toLowerCase();
  return mode === 'free' || mode === '';
}
