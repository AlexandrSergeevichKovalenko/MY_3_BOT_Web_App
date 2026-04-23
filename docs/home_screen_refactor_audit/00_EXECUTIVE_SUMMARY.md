# Home Screen Refactor Audit: Executive Summary

## Scope

This audit covers only the current main/home screen and its navigation surface in the existing single Telegram Mini App. It does not redesign detail sections and does not change runtime behavior.

## Top-Level Recommendation

The app should remain a single app for this change. The current home surface is not a routing system by itself; it is a shell state inside `frontend/src/App.jsx` where the home screen is simply the state `selectedSections.size === 0`. That means the safest refactor is to keep the existing section screens and section state model, and replace only the home-entry/navigation surface.

The current first screen is dominated by three large summary widgets:

- weekly plan
- today tasks
- skills map

Core navigation is mostly hidden behind the hamburger/overlay menu on mobile. This creates a mismatch with the actual product surface, because the app already contains many major destinations:

- translations
- flashcards
- dictionary
- reader
- YouTube
- movies/catalog
- assistant
- support
- analytics
- subscription
- guide
- skill training

The low-risk direction is:

- keep the current shell and `selectedSections` navigation model
- replace the current “summary-first” home with a dashboard that shows direct navigation tiles for the main destinations
- keep some summary widgets below the fold or in a compact strip instead of letting them dominate the first screen

## Main Findings

### Current home screen is narrow and summary-heavy

The actual `HomeScreenSection` currently renders only:

- weekly plan panel
- today tasks panel
- skills map panel

Outside that component, the home state may also show:

- a quick guide card
- onboarding modal
- weekly summary modal
- language-profile / starter-dictionary gates

So the current home screen is not a broad dashboard. It is a summary surface plus gating overlays.

### Current navigation is split across two systems

There are two menu presentations:

- persistent sidebar in the desktop/tablet shell
- hamburger overlay on mobile

Both menus expose roughly the same destinations. The home screen itself does not surface those destinations directly as first-class buttons, except for a few action buttons embedded inside summaries or guide cards.

### Existing section screens are reusable

This is important: the current app already has reusable section refs and navigation helpers:

- `toggleSection`
- `handleMenuSelection`
- `openSingleSectionAndScroll`
- `goHomeScreen`
- `getSectionRefByKey`

This strongly suggests the first implementation can stay frontend-local and reuse existing section screens/routes without backend redesign.

## Mandatory Questions

### 1. Should the app remain a single app for this change?

Yes. Nothing in the current code suggests a need to split the app for this home-screen refactor. The existing state/navigation model is already single-app and can support a tile dashboard.

### 2. Should the hamburger menu be removed entirely, or reduced to profile/settings-only?

It should be reduced, not kept as the primary way to reach core features. Core learning destinations should move to first-screen tiles. The hamburger can remain for:

- guide
- support
- subscription
- analytics
- optional secondary/admin-only destinations

### 3. Which 6–10 destinations most deserve first-screen tiles?

Recommended first-screen tiles:

1. Translations
2. Cards / FSRS
3. Dictionary
4. Reader
5. YouTube
6. Speaking / Assistant
7. Today Tasks
8. Weekly Plan
9. Skills Map
10. Support or Guide

### 4. Which current home blocks should stop dominating the first screen?

- Weekly plan
- Today tasks
- Skills map

They are useful, but they currently consume the whole first screen and suppress direct access to the actual product destinations.

### 5. Is this change primarily frontend-only, or are there backend dependencies that must be respected?

Primarily frontend-only, but it must respect existing backend/data contracts for:

- `/api/today`
- `/api/progress/skills`
- `/api/progress/weekly-plan`
- `/api/progress/plan-analytics`
- `/api/webapp/bootstrap`
- support unread polling
- billing status/plans

No backend redesign is required for the first pass, but these data contracts must remain intact.

### 6. What is the safest first implementation slice?

Replace the visible home composition only:

- keep the current topbar
- keep `selectedSections` navigation
- add a new tile grid on the home state
- keep weekly/today/skills as compact summaries below the tile grid
- keep hamburger as secondary navigation for non-primary destinations

## Best Low-Risk Direction

Build a direct tile-based dashboard that launches existing sections using the current `openSingleSectionAndScroll` / `setSelectedSections` model. Do not touch section internals in the first pass.

## Top Risks

- accidentally breaking `selectedSections` navigation and back behavior
- making the home/dashboard depend on new backend payloads unnecessarily
- breaking snapshot-driven summary refreshes for weekly/today/skills
- hiding support/billing/profile controls without providing equivalent access
- disrupting section restore/back-swipe history

## Recommended Next Implementation Step

Implement a frontend-only home-screen composition pass that:

- keeps the current app single-app
- keeps topbar/profile/help
- introduces visible primary tiles for core sections
- demotes weekly/today/skills from first-screen dominance to secondary summaries
- leaves all existing section screens unchanged
