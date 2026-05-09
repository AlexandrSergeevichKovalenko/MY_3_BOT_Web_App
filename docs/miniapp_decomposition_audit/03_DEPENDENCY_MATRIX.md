# Dependency Matrix

## Legend

- **Strong candidate**: plausible future Mini App with manageable shared-core dependence
- **Shared-heavy**: possible future Mini App, but strongly dependent on platform contracts
- **Keep shared/orchestrated**: should remain in shell/core during early phases

| Domain | Frontend surface | Backend endpoints/domain | Jobs/async | DB/storage | Billing/entitlement | Analytics impact | TTS impact | Auth/session/initData | Cross-module dependencies | Mini App candidacy | Scaling impact if split | Main migration risk |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Translations | `App.jsx` translations section | `/api/webapp/start`, `/check/*`, `/finish`, `/session*`, `/sentences*`, `/history*`, `backend/translation_workflow.py` | translation check/fill/result-side-effects/focus-pool refill | `bt_3_translation*`, daily sentences/translations | `translation_daily_sets`, global entitlement | high | moderate via sentence/audio explain flows | high | dictionary, TTS, today recommendations, history | **Strong candidate** | helps isolate load-heavy async path and translation-specific scaling | session continuity and history consistency |
| Dictionary | dictionary section, quick lookup jump-outs | `/api/webapp/dictionary*`, `/api/mobile/dictionary*` | mostly sync, export-related work | `bt_3_webapp_dictionary*` | global entitlement, indirect feature gating | moderate | moderate for pronunciation | high | translations, reader, YouTube, flashcards | **Shared-heavy** | can improve modularity, less impact on backend scaling alone | losing tight jump-out UX from other modules |
| Flashcards / FSRS | flashcard section | `/api/webapp/flashcards*`, `/api/cards/*`, dictionary cards | cleanup scheduler, bot quiz/reminder integrations | `bt_3_flashcard*` | `feel_word_daily`, global entitlement | moderate | possible pronunciation/audio | high | dictionary-saved words, bot reminders, analytics | **Strong candidate** | helps retention product isolation, limited backend hot-path coupling | breaking dictionary-to-cards pipeline and bot-based repetitions |
| Reader | reader section/library | `/api/webapp/reader/*`, `/api/reader/session/*` | document audio, possible async ingest hooks | `bt_3_reader*` | global entitlement today | moderate | high for reader audio | high | dictionary, analytics, YouTube-style content workflows | **Shared-heavy** | helps isolate document/media operations | preserving library + deep-link continuity |
| YouTube | YouTube and movies sections | `/api/webapp/youtube/*`, today video routes | transcript async jobs, translation path | `bt_3_youtube*` | `youtube_fetch_daily` | moderate-high | low-moderate | high | today plan, dictionary, analytics | **Shared-heavy** | can isolate transcript/media workload | transcript state, quota, catalog, and video-study return paths |
| Today | home/today panel | `/api/today*` | daily plan send, reminders, video recommend, theory prep | daily plan tables + projections | skill-training guard plus global access | high | indirect | high | translations, YouTube, skills, voice, reminders | **Keep shared/orchestrated** | splitting early hurts scaling by duplicating aggregation logic | breaking central home experience |
| Skills / Weekly Plan | home skill panel, weekly plan UI | `/api/progress/skills*`, `/api/progress/weekly-plan`, `/api/progress/plan-analytics`, theory routes | skill-state aggregation, plan projections | `bt_3_skill*` | `skill_training_daily` plus global access | very high | low | high | today, voice skill bridge, analytics | **Keep shared/orchestrated early** | early split hurts more than helps due to shared aggregates | projection drift and cross-domain progress inconsistency |
| Voice Assistant | assistant section + LiveKit runtime | `/api/assistant/*`, `backend/agent.py`, `backend/voice_*` | live runtime + async assessment-style processing | `bt_3_voice*` | likely global plan today | moderate-high | runtime TTS/STT internal | high | skills, today, analytics, user profile | **Shared-heavy late candidate** | separate runtime may help ops isolation later | duplicated auth/context/runtime orchestration |
| TTS | hidden cross-module UI support | `/api/webapp/tts*`, internal TTS modules | prewarm, recovery, cleanup, quota control | `bt_3_tts*` | `tts_chars_daily` | moderate | core service itself | high | translations, dictionary, reader, bot | **Shared core, not separate user Mini App** | centralizing helps scaling; splitting as product surface hurts | fragmentation of audio semantics and cost control |
| Image Quiz / Games | mostly Telegram-facing | image-quiz utils + bot callbacks | prepare/render/refresh jobs | `bt_3_image_quiz*` | likely global today | moderate | low | high | bot callbacks, dictionary/word material | **Not first Mini App candidate** | possible isolated worker scaling, but low UI leverage | orphaned Telegram quiz UX |
| Analytics | analytics section | `/api/webapp/analytics/*`, `/api/economics/summary` | private/group summary schedulers | mixed aggregates | global plan/visibility | very high | low | high | every domain | **Keep shared/orchestrated** | separate UI may help later, but backend remains aggregate-heavy | broken cross-domain visibility |
| Billing / Subscription | subscription section | `/api/billing/*`, webhook, debug/admin | webhook + Stripe lifecycle | `plans`, `user_subscriptions`, `plan_limits` | core concern | high | indirect via limits | very high | all domains | **Shared core only** | splitting hurts consistency | inconsistent entitlements across apps |
| Support / Telegram support | support section + bot | `/api/webapp/support/*`, bot handlers | scheduled/admin messaging | `bt_3_support*` | global access today | low-moderate | none | high | all domains | **Keep shared/core-adjacent** | little scaling gain from split | fragmented support context |

## High-Coupling Areas

- Today
- Skills / Weekly Plan
- Analytics
- Billing / Entitlement
- Telegram bot reminder and summary flows
- TTS shared service

## Lower-Coupling Areas

- Translations
- Flashcards / personal word training

## Implication

The best early decomposition candidates are the domains with:

- strong user identity
- strong backend clustering
- lower dependence on aggregate projections
- lower need to coordinate scheduled reminders across many features

That points to:

1. Translations
2. Flashcards / Vocabulary training

And explicitly not:

1. Today / shell
2. Billing
3. Analytics
