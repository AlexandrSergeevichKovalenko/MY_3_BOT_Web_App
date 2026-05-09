# New Dashboard Proposal

## Goal

Replace the current summary-dominant home screen with a direct tile-based dashboard while reusing existing section screens and the current single-app navigation model.

## Recommended Header Structure

Keep in header:

- title
- avatar/profile access
- language toggle
- theme toggle
- language-pair button
- help button

Change:

- hamburger should stop being the primary way to reach core study sections

## Recommended Hamburger Role

Reduce to utility/secondary navigation:

- guide
- support
- analytics
- subscription
- conditional/internal items like economics

Optional:

- keep Movies there until product decides whether it deserves a tile

## Recommended First-Screen Tile Grid

Recommended visible tiles:

1. Translations
2. Cards / FSRS
3. Dictionary
4. Reader
5. YouTube / Video
6. Speaking / Assistant
7. Today Tasks
8. Weekly Plan
9. Skills Map

Optional 10th tile depending on product choice:

- Support
- Guide
- Movies

## Recommended Summary Strip / Secondary Widgets

Below the tile grid, keep compact summaries for:

- Today
- Weekly Plan
- Skills

But demote them from full first-screen dominance.

Recommended summary form:

- small compact cards with one headline metric and one CTA
- preserve refresh and key action buttons
- avoid rendering large detailed bodies above the fold

## What Should Stay Below the Fold

- expanded weekly-plan editor
- detailed today task list
- full skills ring legend

These can still exist on the home surface, but they should not be the first thing the user sees before core destinations.

## What Should Not Be on the First Screen Anymore

- full-size weekly goal editor
- long today task list as the primary entry impression
- large skills ring block dominating initial viewport

## What Can Reuse Existing Routes/Screens with Minimal Churn

All tile navigation can reuse existing section flows via:

- `openSingleSectionAndScroll`
- `setSelectedSections(new Set([key]))`
- existing refs:
  - `translationsRef`
  - `flashcardsRef`
  - `dictionaryRef`
  - `readerRef`
  - `youtubeRef`
  - `assistantRef`
  - `analyticsRef`
  - `billingRef`
  - `skillTrainingRef`

This is the key low-risk opportunity. The dashboard does not need new section routes in the first pass.

## Recommended Classification

### Primary tiles

- Translations
- Cards / FSRS
- Dictionary
- Reader
- YouTube
- Assistant
- Today
- Weekly Plan
- Skills

### Secondary summaries

- Weekly Plan summary
- Today summary
- Skills summary

### Secondary menu/profile/header items

- Guide
- Support
- Analytics
- Subscription
- Language pair
- Theme/language/profile

### Needs product decision

- Movies
- Skill Training

## Recommended UX Direction

The new home should feel like:

- immediate destination selection first
- status/summary second
- detailed plan widgets after that

That better matches the actual breadth of the product without redesigning the underlying sections.
