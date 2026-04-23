# Open Questions and Decisions

## Product Decisions Needed

### 1. What exactly is a “module” commercially?

Need decision:

- are modules product domains, UI tabs, or entitlement packages?

### 2. What should partial-access users see on the home shell?

Need decision:

- should Today include tasks from locked modules?
- should locked tasks be hidden, replaced, or upsell-decorated?

### 3. Is trial global or module-specific in the future?

Need decision before modular subscription implementation.

### 4. Should Reader and YouTube be separate Mini Apps or one content-study app?

The codebase suggests overlap, but product intent is not definitive from code alone.

### 5. Should Voice remain inside the shell permanently?

Technically it could become separate later, but product evidence for that is not yet conclusive from the repo alone.

## Architectural Questions

### 1. What is the exact cross-module navigation contract?

Need explicit decision on:

- launch parameters
- return parameters
- shell deep links

### 2. Which shared user context is loaded once vs per-module?

Examples:

- language profile
- billing status
- support unread count
- home snapshot data

### 3. Where should module analytics events be normalized?

Need a single event taxonomy owner.

### 4. Which projection-backed surfaces remain shell-owned?

Likely:

- today
- skills
- weekly plan

But this should be explicitly decided.

## Unclear or Ambiguous from Code Alone

- exact long-term product importance of “movies/catalog” vs YouTube main study path
- whether some admin surfaces are temporary or permanent operational tools
- whether all current voice flows are production-critical for all users or only a subset

These should be clarified by product owners before implementation.

## Required Decisions Before Implementation

1. approve the shell-as-orchestrator approach
2. approve the first extraction candidate
3. approve the conceptual entitlement model direction
4. define cross-module navigation contract
5. define shell behavior for partially entitled users
