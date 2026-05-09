# Current Home Screen Inventory

## Exact Current Home Screen Implementation

Main home component:

- `frontend/src/App.jsx`
- `HomeScreenSection`

Home-state condition:

- `isHomeScreen = !flashcardsOnly && selectedSections.size === 0`

This means the current home screen is not an independent route. It is the “no section selected” state of the main shell.

## Current Home-Screen Content

## 1. Weekly Plan Block

### Where

- `HomeScreenSection` in `frontend/src/App.jsx`
- panel class: `weekly-plan-panel`

### Purpose

- show weekly goals and progress
- allow editing/saving weekly goals
- show metrics and forecast

### Informational or action-oriented

- both
- informational summary plus direct editing action

### API/data dependency

- `/api/progress/weekly-plan`
- `/api/progress/plan-analytics`
- local snapshot cache in localStorage

### Action buttons

- refresh
- collapse/expand
- save weekly plan
- period selection

### Classification for new home

- **KEEP AS SECONDARY SUMMARY**

### Why

- valuable overview block
- not a core navigation destination by itself
- too large to dominate first-screen entry
- editing form is useful, but not the best first thing users should see before reaching core learning modes

### Current problem

- occupies major first-screen space
- behaves more like a detailed dashboard card than a quick home launcher

## 2. Today Tasks Block

### Where

- `HomeScreenSection`
- panel class: `today-plan-panel`

### Purpose

- show the current daily route
- allow start/regenerate/refresh
- expose task-specific progress and actions

### Informational or action-oriented

- both, but strongly action-oriented

### API/data dependency

- `/api/today`
- `/api/today/regenerate`
- `/api/today/items/<id>/start`
- `/api/today/items/<id>/timer`
- `/api/today/video/feedback`
- local snapshot cache in localStorage

### Action buttons

- regenerate plan
- refresh
- start task
- video feedback thumbs

### Classification for new home

- **KEEP AS PRIMARY TILE** plus **KEEP AS SECONDARY SUMMARY**

### Why

- “Today” deserves a direct first-screen tile as a top-level destination
- the current detailed list should not dominate first screen
- a compact summary preview is useful, but the entry point should be a tile, not a long task list

### Current problem

- the block is useful, but it forces the home screen into a productivity-dashboard shape before the user can reach core learning areas

## 3. Skills Map Block

### Where

- `HomeScreenSection`
- panel class: `skill-report-panel`

### Purpose

- display weak/strong skill rings
- show training status
- offer skill practice/resume actions

### Informational or action-oriented

- mostly informational with direct practice actions

### API/data dependency

- `/api/progress/skills`
- `/api/progress/skills/<skill_id>/practice/start`
- snapshot cache in localStorage

### Action buttons

- refresh
- train skill
- resume training

### Classification for new home

- **KEEP AS PRIMARY TILE** plus **KEEP AS SECONDARY SUMMARY**

### Why

- “Skills” is a real destination and should have a visible tile
- the current rings visualization is useful as a compact summary
- the full ring layout is too large for first-screen dominance

### Current problem

- strong visual emphasis, but secondary to direct navigation if the home screen becomes a dashboard

## Additional Home-State Surfaces Outside `HomeScreenSection`

## 4. Quick Guide Card

### Where

- `showHomeGuideQuickCard` block in `App.jsx`
- class: `hero-guide-card`

### Purpose

- onboarding/guide shortcut

### Informational or action-oriented

- action-oriented

### Classification

- **MOVE OUT OF HOME SURFACE** or keep as dismissible secondary helper

### Why

- useful for onboarding, not for permanent first-screen dominance

## 5. Onboarding Modal

### Where

- onboarding overlay/modal in `App.jsx`

### Purpose

- first-run education

### Classification

- **KEEP OUTSIDE MAIN DASHBOARD**

### Why

- onboarding is modal/gated, not a dashboard block

## 6. Weekly Summary Modal

### Where

- `WeeklySummaryModal` in `App.jsx`

### Purpose

- periodic summary, comparison, recommendation

### Classification

- **MOVE OUT OF HOME SURFACE**

### Why

- this is a modal experience, not a persistent home dashboard block

## 7. Language Profile Gate / Starter Dictionary Gate

### Where

- modal gates in `App.jsx`

### Purpose

- required setup and starter dictionary onboarding

### Classification

- **KEEP IN HEADER/PROFILE/SETUP FLOW**

### Why

- they are setup gates, not dashboard navigation content

## Current User Value of the Home Screen

The current home screen does provide value:

- fast overview of plan, tasks, and skills
- direct actions for today and skills
- snapshot-backed data with background refresh

But it under-serves navigation. The app has many major destinations, and most of them are hidden behind hamburger access instead of visible on first load.

## Current Home-Screen Problems

1. The first screen is summary-first, not destination-first.
2. Core product areas are hidden in hamburger/overlay navigation.
3. The home surface is vertically heavy before the user reaches core actions.
4. Today, weekly plan, and skills are all valuable, but together they crowd out direct entry points.
5. The home screen does not reflect the breadth of the actual product surface.
