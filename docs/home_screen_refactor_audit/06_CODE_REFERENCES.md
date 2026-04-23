# Code References

## Primary Frontend Files

- `frontend/src/App.jsx`
- `frontend/src/App.css`
- `frontend/src/i18n.js`

## Home Screen

- `frontend/src/App.jsx`
  - `HomeScreenSection`
  - `isHomeScreen = !flashcardsOnly && selectedSections.size === 0`

Key home panel blocks:

- `weekly-plan-panel`
- `today-plan-panel`
- `skill-report-panel`

## Header / Topbar

- `frontend/src/App.jsx`
  - `webapp-topbar`
  - hamburger `menu-toggle`
  - `topbar-title`
  - avatar button
  - language toggle
  - theme toggle
  - `language-pair-button`
  - `topbar-help-button`

Related styles:

- `frontend/src/App.css`
  - `.webapp-topbar`
  - `.menu-toggle`
  - `.topbar-title`
  - `.language-pair-button`
  - `.topbar-help-button`

## Hamburger / Sidebar Menu

- `frontend/src/App.jsx`
  - persistent sidebar menu inside `webapp-sidebar`
  - mobile overlay menu inside `overlay-menu`

Destination handlers:

- `toggleSection`
- `handleMenuSelection`
- `openSingleSectionAndScroll`
- `goHomeScreen`
- `getSectionRefByKey`

Related styles:

- `frontend/src/App.css`
  - `.webapp-sidebar`
  - `.overlay-menu`
  - `.menu-item`
  - `.menu-item.is-active`

## Home Data Loaders

- `loadTodayPlan`
- `loadSkillReport`
- `loadWeeklyPlan`
- `loadPlanAnalytics`

Related snapshot helpers:

- `readWeeklyPlanSnapshot`
- `persistWeeklyPlanSnapshot`
- `readTodayPlanSnapshot`
- `persistTodayPlanSnapshot`
- `readSkillReportSnapshot`
- `persistSkillReportSnapshot`

## Home/Navigation State

- `selectedSections`
- `menuOpen`
- `menuMultiSelect`
- `currentSingleSectionRouteKey`
- `sectionRouteHistoryRef`
- `goBackToPreviousSection`

## Support Badge / Utility Dependencies

- `loadSupportUnread`
- `loadSupportMessages`
- `markSupportMessagesRead`
- `supportUnreadCount`

## Relevant Backend Endpoints Used by Home Surface

- `/api/webapp/bootstrap`
- `/api/user/language-profile`
- `/api/webapp/starter-dictionary/status`
- `/api/webapp/starter-dictionary/apply`
- `/api/today`
- `/api/today/regenerate`
- `/api/today/items/<id>/start`
- `/api/today/items/<id>/timer`
- `/api/today/video/feedback`
- `/api/progress/skills`
- `/api/progress/skills/<skill_id>/practice/start`
- `/api/progress/skills/<skill_id>/practice/event`
- `/api/progress/weekly-plan`
- `/api/progress/plan-analytics`
- `/api/webapp/support/unread`
- `/api/billing/status`
- `/api/billing/plans`

## Menu Destination Section Refs

- `translationsRef`
- `youtubeRef`
- `moviesRef`
- `dictionaryRef`
- `readerRef`
- `flashcardsRef`
- `assistantRef`
- `supportRef`
- `analyticsRef`
- `economicsRef`
- `billingRef`
- `guideRef`
- `skillTrainingRef`
