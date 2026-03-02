# 🇩🇪 DeutschFlow

> **Telegram Bot + Telegram Mini App + Live Voice Agent** for learning German through translations, stories, YouTube, reading, dictionary work, flashcards, analytics, goals, and real-time speaking practice.

<p align="center">
  <strong>One ecosystem for German learning:</strong><br>
  🤖 Telegram bot • 📱 Mini App • 🎙️ Live speaking agent • 📚 Dictionary • 🧠 FSRS cards • 🎬 YouTube subtitles • 📖 Reader • 📊 Analytics • 💸 Economics • 🧾 Subscription
</p>

---

## ✨ What This Project Is

**DeutschFlow** is a full language-learning platform built around Telegram.

It combines:

- **a Telegram bot** for daily learning, quizzes, translation checking, dictionary flows, access control, reminders, and group interaction;
- **a Telegram Web App / Mini App** for deep interactive practice;
- **a LiveKit-powered voice agent** for real-time spoken German;
- **a Python backend** with LLM workflows, analytics, billing, TTS, transcript handling, FSRS review logic, and scheduled jobs;
- **a React frontend** that turns the Telegram Mini App into a rich study dashboard.

This README reflects the functionality present in:

- [`bot_3.py`](./bot_3.py)
- [`backend/backend_server.py`](./backend/backend_server.py)
- [`backend/agent.py`](./backend/agent.py)
- [`backend/api.py`](./backend/api.py)
- [`backend/database.py`](./backend/database.py)
- [`backend/openai_manager.py`](./backend/openai_manager.py)
- [`backend/translation_workflow.py`](./backend/translation_workflow.py)
- [`frontend/src/App.jsx`](./frontend/src/App.jsx)

---

## 🚀 Core Product Surfaces

### 🤖 Telegram Bot

The bot is not just a launcher for the Mini App. It has its own rich learning flows:

- **Access control system**
  - `/start`
  - `/request_access`
  - admin approval / rejection / deferral
  - pending access queue
  - allowed user management
- **Telegram-based translation checking**
  - users send translations directly in chat
  - answers are checked and scored
  - mistakes can be explained in more detail
- **Inline GPT grammar explanation**
  - explanation buttons under checked answers
  - expanded grammatical reasoning for user submissions
- **Private dictionary lookup**
  - word or phrase lookup directly in Telegram private chat
  - language-pair selection
  - rich dictionary cards
  - multiple save options / collocation variants
- **Vocabulary saving workflow**
  - save dictionary results into the learning database
  - folder-aware saving logic
- **Telegram poll quizzes**
  - scheduled quizzes
  - active quiz tracking
  - poll answer handling
  - private feedback prompts
- **YouTube recommendation flow**
  - topic-based German-learning video recommendations
  - preferred curated YouTube channels
- **Analytics delivery inside Telegram**
  - personal analytics charts
  - comparison analytics between users
  - automated summary sending
- **Flashcard reminders**
  - timed review nudges for users
- **Support relay**
  - admins can reply to user support messages from Telegram
  - message threading is preserved via stored metadata
- **Web App deep links**
  - direct launch into Mini App
  - review-focused deeplinks
- **System message tracking**
  - sent Telegram messages are tracked for later cleanup

### 📱 Telegram Mini App / Web App

The Mini App is the largest product surface in the repository. It includes:

#### 📝 Translation Training

- topic-based sentence generation
- level-based difficulty selection
- multilingual language-pair support
- quick translation checking
- history of submissions
- daily translation history
- finish flow with result summary
- in-app GPT explanation endpoint

#### 🕵️ Mystery Story Mode

- special **“mystery story”** learning format
- story session start / submit / finish flow
- structured feedback by sentence
- story history
- per-sentence correction logic
- audio post-processing support for story corrections

#### 📚 Dictionary

- multilingual dictionary lookup
- direction-aware lookup by source and target language
- parts of speech, articles, forms, prefixes, usage examples
- collocations generation
- saving entries into personal vocabulary
- vocabulary folders
- folder creation with icons and colors
- PDF export of vocabulary
- random and reusable dictionary data for flashcards
- text normalization endpoint for selection lookup

#### 🧠 Flashcards & Review

- classic flashcard set mode
- **FSRS spaced repetition**
- quiz mode
- block-building mode
- sentence completion mode
- prefetch queue for smooth review
- answer logging
- card enrichment
- feel / emotional feedback flow
- private “feel” message dispatch
- FSRS review logging and queue management

#### 🎬 YouTube Study Mode

- YouTube search inside the app
- curated catalog flow
- transcript fetching
- subtitle translation
- bilingual subtitle display
- overlay mode on top of video
- manual transcript insertion
- subtitle timing support
- watch-focus mode
- movie / saved-video catalog with language filtering
- selection-based lookup from subtitles

#### 📖 Reader

- ingest content from:
  - pasted text
  - uploaded files
  - URLs
- language detection for imported content
- personal reader library
- rename / archive / delete library documents
- reading progress sync
- bookmark sync
- immersive reading mode
- vertical and horizontal reading modes
- page segmentation
- swipe sensitivity settings
- font size and weight controls
- tracked reading timer
- audio export for documents
- offline TTS generation for reading content

#### 🎙️ Live Voice Assistant

- connect to a real-time speaking room
- live German speaking practice
- session tracking for assistant minutes
- clean connect / disconnect flow
- billing-aware assistant session logging
- integration with LiveKit room UI

#### 🧩 Theory & Skill Training

- skill progress API
- skill practice start / event tracking
- daily theory preparation
- theory explanation packages
- theory practice with 5 generated sentences
- answer checking and structured feedback
- skill-based recommendations:
  - theory resources
  - YouTube video
  - practice block

#### 📅 Today Plan & Weekly Goals

- generated daily plan
- plan item timers
- start / pause / resume / complete logic
- reminders
- weekly goals:
  - translations
  - learned words
  - assistant minutes
  - reading minutes
- weekly plan analytics
- progress-to-goal charts

#### 💬 Support

- in-app support chat
- unread badge
- message retry for failed sends
- user/admin timeline rendering

#### 📊 Analytics

- summary analytics
- time-series analytics
- user-vs-user comparison analytics
- rank inside leaderboard
- completion metrics
- missed-day metrics
- score and time metrics

#### 💸 Economics

- personal cost dashboard
- variable vs fixed costs
- provider breakdown
- action-type breakdown
- voice cost breakdown:
  - Whisper STT
  - agent TTS
  - LiveKit room minutes
- cost-per-event metrics
- active-user allocation models

#### 🧾 Subscription

- billing plans list
- plan status
- current entitlement view
- Stripe checkout session creation
- Stripe portal session creation
- daily cap / spending status
- free vs paid plan visibility

#### 🎯 Selection Menu Everywhere

The Mini App supports text selection actions in learning surfaces such as translations, YouTube and Reader:

- quick save to dictionary
- inline dictionary lookup
- GPT explanation
- TTS for selected content

### 🎙️ Live Voice Agent

The repository also contains a dedicated **LiveKit agent**:

- built on **LiveKit Agents**
- uses:
  - **OpenAI LLM**
  - **Whisper STT**
  - **OpenAI TTS**
  - **Silero VAD**
- optimized pipeline reuse to reduce cold starts
- conversation transcript logging
- disconnect timeout handling
- session cleanup when participants leave
- personalized teaching behavior
- tool access to user’s recent Telegram mistakes

#### 🛠️ Agent Intelligence

The live agent can query structured learning history via [`GermanTeacherTools`](./backend/api.py):

- recent mistake categories
- most common error themes
- example user mistakes
- user-specific focus areas for live conversation

That means the speaking agent is not generic. It can adapt conversation to the learner’s real written error history.

---

## 🧠 Backend Intelligence Layer

The backend is much more than a simple API. It includes:

- **LLM gateway orchestration**
  - OpenAI Assistants
  - Responses API hybrid mode
  - task-specific prompts
  - usage tracking
- **translation workflows**
  - translation checking
  - rechecking
  - level-aware sentence generation
  - mystery story generation and parsing
  - correct-answer extraction
- **subtitle workflows**
  - transcript fetch
  - translation of subtitles
  - manual transcript fallback
- **TTS workflows**
  - chunking
  - chunk validation
  - TTS audio caching
  - TTS prewarm jobs
- **reader ingestion workflows**
  - normalization
  - PDF/text/url ingestion
  - book-state persistence
- **dictionary workflows**
  - lookup cache
  - collocation generation
  - export support
- **FSRS state management**
  - due card count
  - next card
  - review logging
  - new-card pacing
- **skill / plan / progress data model**
- **economics and billing telemetry**
  - event logging
  - pricing snapshots
  - fixed-cost allocation
  - live usage accounting
- **Telegram auth validation**
- **mobile auth exchange**
- **browser Telegram login support**

---

## 🏆 Gamification & Social Features

The project contains group and personal motivation mechanics:

- weekly leaderboard
- weekly badges
- champion of the week
- precision master
- streak-based recognition
- daily private analytics
- group audio dispatches
- today-plan reminders
- evening reminders
- weekly goal summaries

---

## 🧰 Admin & Operations

There is significant internal tooling for operations:

- access request moderation
- billing debug and entitlement debug
- fixed-cost sync
- price snapshot sync
- manual economics event creation
- manual daily audio dispatch
- manual private analytics dispatch
- manual prewarm jobs
- manual weekly goals and weekly badges dispatch
- skill resource domain admin endpoints
- YouTube transcript proxy access management
- YouTube debug endpoint
- system message cleanup
- flashcard feel cleanup

---

## 🏗️ Architecture

```text
Telegram User
   │
   ├── 🤖 Telegram Bot (python-telegram-bot)
   │       ├── direct chat learning flows
   │       ├── group reports / reminders / quizzes
   │       └── Mini App launch
   │
   ├── 📱 Telegram Mini App (React + Vite)
   │       ├── translations
   │       ├── dictionary
   │       ├── YouTube subtitles
   │       ├── reader
   │       ├── flashcards / FSRS
   │       ├── theory / skill training
   │       ├── analytics / economics / subscription
   │       └── support chat
   │
   └── 🎙️ Live Voice Agent (LiveKit)
           ├── Whisper STT
           ├── OpenAI LLM
           ├── OpenAI TTS
           └── personalized tools from DB

Backend Services
   ├── Flask API
   ├── PostgreSQL
   ├── OpenAI
   ├── LiveKit
   ├── Stripe
   ├── YouTube APIs / transcript tools
   ├── Google Cloud TTS
   └── APScheduler jobs
```

---

## 🗂️ Repository Structure

```text
.
├── bot_3.py                    # Main Telegram bot logic
├── backend/
│   ├── backend_server.py       # Main Flask backend and Mini App API
│   ├── agent.py                # LiveKit speaking agent
│   ├── api.py                  # Agent tools exposed to LLM
│   ├── database.py             # Data model, persistence, analytics, billing tables
│   ├── openai_manager.py       # LLM prompts and execution layer
│   ├── translation_workflow.py # Translation/story workflows
│   ├── analytics.py            # User/comparison analytics
│   └── srs/                    # FSRS scheduler logic
├── frontend/
│   ├── src/App.jsx             # Main Telegram Mini App UI
│   ├── src/App.css             # Main styling
│   └── package.json            # Frontend dependencies and scripts
├── Dockerfile.backend          # Full backend + built frontend image
├── Dockerfile.agent            # Voice agent image
└── Procfile                    # Gunicorn startup for deployment
```

---

## 🛠️ Tech Stack

### Backend

- **Python**
- **Flask**
- **PostgreSQL / psycopg2**
- **APScheduler**
- **OpenAI**
- **Stripe**
- **youtube-transcript-api**
- **yt-dlp**
- **spaCy**
- **ReportLab**
- **Google Cloud Text-to-Speech**
- **pyttsx3**
- **pydub**
- **pypdf**
- **FSRS**

### Frontend

- **React 18**
- **Vite**
- **ECharts**
- **LiveKit React Components**
- **PWA support via vite-plugin-pwa**

### Voice Layer

- **LiveKit**
- **Whisper STT**
- **OpenAI TTS**
- **Silero VAD**

---

## 🔐 Authentication & Security

The project includes multiple auth layers:

- Telegram `initData` validation for Mini App
- browser Telegram auth flow
- mobile auth token exchange
- access-request approval flow before full usage
- allowed-user lists
- admin-only control endpoints

---

## ⚙️ Running the Project

### 1. Clone the repository

```bash
git clone <your-repo-url>
cd TELEGRAM_BOT_DEUTSCHESPRACHE
```

### 2. Backend setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
```

### 3. Frontend setup

```bash
cd frontend
npm install
npm run dev
```

### 4. Run the Flask backend

From the project root:

```bash
gunicorn -b 0.0.0.0:$PORT --timeout 300 --graceful-timeout 30 backend.backend_server:app
```

### 5. Run the Telegram bot

```bash
python3 bot_3.py
```

### 6. Run the voice agent

Use the dedicated agent environment and start the LiveKit worker from [`backend/agent.py`](./backend/agent.py).

### 7. Docker deployment

The repository already includes:

- [`Dockerfile.backend`](./Dockerfile.backend)
- [`Dockerfile.agent`](./Dockerfile.agent)
- [`Procfile`](./Procfile)

So the project is already structured for containerized / platform deployment.

---

## 🔑 Environment Variables

The codebase uses a large set of environment variables. At minimum, the platform relies on values such as:

- `TELEGRAM_Deutsch_BOT_TOKEN`
- `DATABASE_URL_RAILWAY`
- `OPENAI_API_KEY`
- `YOUTUBE_API_KEY`
- `MOBILE_AUTH_SECRET`
- `BOT_ADMIN_TELEGRAM_IDS`
- `AUDIO_DISPATCH_TOKEN`
- Stripe-related keys
- LiveKit keys
- Google Cloud TTS credentials

See:

- [`.env.example`](./.env.example)

---

## 🌍 Product Positioning

This is not a single-purpose “translation checker”.

**DeutschFlow** is a **multi-surface German learning platform** with:

- structured written practice,
- live speaking practice,
- vocabulary acquisition,
- reading immersion,
- video-based subtitle learning,
- spaced repetition,
- progress analytics,
- cost visibility,
- subscription infrastructure,
- Telegram-native engagement loops.

---

## 🎯 Why This Project Is Interesting

- It merges **Telegram-native UX** with a **serious learning backend**.
- It combines **chat**, **Mini App**, and **voice room** in one product.
- It already includes **gamification**, **billing**, **analytics**, **FSRS**, **support**, and **ops tooling**.
- It is designed not only for users, but also for **admins**, **moderation**, and **scheduled automation**.

---

## 📌 Current Highlights

- ✅ Telegram bot with real learning flows
- ✅ Rich Telegram Mini App
- ✅ Live German voice teacher
- ✅ YouTube subtitle learning
- ✅ Reader with offline audio export
- ✅ Dictionary with folders and PDF export
- ✅ FSRS review engine
- ✅ Daily plans and weekly goals
- ✅ Analytics, economics and subscription layer
- ✅ Admin and scheduler infrastructure

---

## ❤️ In One Sentence

**DeutschFlow turns Telegram into a full German-learning operating system.**
