# Home Screen Dependency Map

## 1. Frontend Files and Components

Primary files:

- `frontend/src/App.jsx`
- `frontend/src/App.css`
- `frontend/src/i18n.js`

Primary home-related component:

- `HomeScreenSection` in `App.jsx`

Header/topbar and hamburger are also implemented directly inside `App.jsx`, not in isolated reusable components.

## 2. Home Routing / Navigation Model

Home state:

- `isHomeScreen = !flashcardsOnly && selectedSections.size === 0`

Primary navigation helpers:

- `toggleSection`
- `handleMenuSelection`
- `openSingleSectionAndScroll`
- `openSectionAndScroll`
- `goHomeScreen`
- `getSectionRefByKey`

Back-navigation mechanics:

- `currentSingleSectionRouteKey`
- `sectionRouteHistoryRef`
- edge-swipe back handling

## 3. Current Header / Topbar Dependencies

Header elements currently include:

- hamburger button
- title
- avatar upload/profile button
- language toggle
- theme toggle
- language-pair button
- help button

Related state/dependencies:

- `menuOpen`
- `userAvatar`
- `uiLang`
- `themeMode`
- `languageProfileModalOpen`
- `openGuideSection`
- `openLanguageProfileModal`

## 4. Current Hamburger Dependencies

Menu state:

- `menuOpen`
- `menuMultiSelect`
- `selectedSections`
- `supportUnreadCount`
- `canViewEconomics`
- `flashcardsOnly`

The menu is not a dumb UI shell. It directly changes section state and sometimes additional behavior:

- opening flashcards also mutates flashcard mode/session state
- opening YouTube stores a back-section candidate
- multi-select mode changes navigation semantics

## 5. Home-Screen Data Fetching

### Home blocks

- `loadTodayPlan`
  - endpoint: `/api/today`
  - related actions: `/api/today/regenerate`, `/api/today/items/*`, `/api/today/video/feedback`
- `loadSkillReport`
  - endpoint: `/api/progress/skills`
  - related actions: `/api/progress/skills/<skill_id>/practice/start`
- `loadWeeklyPlan`
  - endpoint: `/api/progress/weekly-plan`
- `loadPlanAnalytics`
  - endpoint: `/api/progress/plan-analytics`

### Global shell/bootstrap

- `/api/webapp/bootstrap`
- `/api/user/language-profile`
- starter dictionary status/apply endpoints

### Header/menu utility data

- support unread:
  - `/api/webapp/support/unread`
- billing data when subscription section opens:
  - `/api/billing/status`
  - `/api/billing/plans`

## 6. Snapshot / Local Cache Dependencies

Current home screen uses local snapshot persistence:

- weekly plan snapshot
- today plan snapshot
- skill report snapshot

Storage keys are user/date-aware and are read before background refresh.

Implication:

- simple UI reordering is low risk
- replacing the home composition must preserve the timing and visibility of these snapshot-backed widgets if they stay on the home surface

## 7. Loading and Error States

Each current home block has explicit:

- loading state
- skeleton state
- stale/fresh snapshot tone
- error state
- “empty” handling

This means the current home is not static markup. Any refactor that moves these blocks must preserve:

- skeleton behavior
- snapshot freshness labels
- background refresh timing

## 8. Entitlement / Billing Dependencies

Direct home-block rendering is not strongly billing-gated, but the shell around it is affected by:

- subscription section availability
- billing status/plans loading
- language-profile gating
- starter dictionary onboarding prompts

The home refactor should not assume that topbar and utilities are purely decorative.

## 9. Analytics / Tracking / Performance

Observed instrumentation:

- `useAppPerfRenderProbe('HomeScreenSection', ...)`
- `PerfProfiler` wrappers

The home screen and sections are performance-instrumented. Reordering UI is frontend-local, but removing or deeply restructuring these surfaces can affect existing perf/debug instrumentation.

No dedicated home-specific product analytics event was clearly identified in this pass, but performance probes and section-route state are definitely present.

## 10. Telegram-Specific Dependencies

- `telegramApp` integration for WebApp environment
- fullscreen handling on Telegram tablet-like clients
- initData bootstrapping
- single-instance claim/release and lease heartbeat

These are shell-level concerns. A home-screen UI refactor must not break them.

## 11. Performance-Sensitive Areas

Potentially sensitive:

- startup sequencing for language profile and home block refreshes
- snapshot-first rendering with delayed background refresh
- support unread polling
- section-route history/back gestures
- multi-section rendering when `menuMultiSelect` is enabled

## 12. What Would Break if the UI Is Reordered Naively

- section open/close semantics if tile clicks bypass `selectedSections`
- back-swipe history if route-key transitions are not preserved
- flashcards open behavior if tile click does not mirror current flashcard setup mutations
- YouTube back-section behavior if launch is handled inconsistently
- support unread visibility if support moves but badge logic is ignored
