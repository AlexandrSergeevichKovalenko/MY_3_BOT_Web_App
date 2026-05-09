# Hamburger Menu Inventory

## Current Menu Implementations

There are two menu surfaces in `frontend/src/App.jsx`:

- persistent sidebar menu in the desktop/tablet shell
- hamburger-triggered overlay menu on mobile

They expose essentially the same destinations.

## Current Destinations

## 1. Today

### Purpose

- return to home state / daily dashboard

### Classification

- **Should become a visible home tile**

### Why

- it is a core destination, not a secondary setting

## 2. Guide

### Purpose

- app usage guide and onboarding reference

### Classification

- **Should remain secondary**

### Why

- useful, but not a primary learning destination

## 3. Translations

### Purpose

- translation practice and story mode

### Classification

- **Should become a visible home tile**

## 4. YouTube

### Purpose

- transcript/video study

### Classification

- **Should become a visible home tile**

## 5. Movies

### Purpose

- curated catalog feeding into YouTube study

### Classification

- **NEEDS PRODUCT DECISION**

### Why

- it is a destination today, but may belong either:
  - as its own tile
  - as a sub-entry inside YouTube/Video

## 6. Dictionary

### Purpose

- lookup/save/folder/export

### Classification

- **Should become a visible home tile**

## 7. Reader

### Purpose

- ingest/library/reading/audio

### Classification

- **Should become a visible home tile**

## 8. Flashcards

### Purpose

- cards/FSRS/training modes

### Classification

- **Should become a visible home tile**

## 9. Assistant

### Purpose

- speaking practice / voice assistant

### Classification

- **Should become a visible home tile**

## 10. Support

### Purpose

- user support chat with unread badge

### Classification

- **Should remain secondary**

### Why

- important access point, but not a primary first-screen learning tile
- still needs visible access, likely via header utility area or a smaller tile if product wants stronger support visibility

## 11. Analytics

### Purpose

- progress analytics and comparisons

### Classification

- **Should remain secondary or become a lower-priority tile**

### Why

- strong destination, but not as primary as learning entry points

## 12. Subscription

### Purpose

- billing status, plans, Stripe management

### Classification

- **Should remain secondary**

### Why

- important utility/settings destination, not a core study entry point

## 13. Economics

### Purpose

- admin-only/internal economics surface

### Classification

- **Should stay hidden from main dashboard**

### Why

- already gated by `canViewEconomics`
- not a general-user home tile

## 14. Skill Training

### Purpose

- focused training for one weak skill

### Classification

- **NEEDS PRODUCT DECISION**

### Why

- it is currently more like a context-driven subflow than a broad standalone destination
- likely better reached from Skills or Today rather than as a permanent top-row tile

## Summary Classification

### Strong tile candidates

- Today
- Translations
- Dictionary
- Reader
- Flashcards
- YouTube
- Assistant
- Skills
- Weekly Plan

### Secondary/header/profile-level

- Guide
- Support
- Analytics
- Subscription

### Hidden or conditional

- Economics

### Needs product decision

- Movies
- Skill Training
