# 🛡️ Security Deep-Dives — the code, block by block, with a security lens

> This is a series of learning files. We take **one logical block** of the app at a time
> (sentence translation, payments, dictionary, reader, the YouTube block, TTS, voice…) and
> explain it from two sides at once: **how it actually works inside**, and **how someone would
> attack it / how we defend against that**.
>
> It's a continuation of [LEARNING_PATH.md](../LEARNING_PATH.md) (the overall study route) and it
> follows the same "reference" style as [autosave_scaling_explained.md](../autosave_scaling_explained.md):
> plain language, real analogies, `file:line` links, and code snippets pasted right into the text
> so you never have to go hunting.

---

## How to read this series

First go through [LEARNING_PATH.md](../LEARNING_PATH.md), Levels 0–2 (base technologies + one
end-to-end trace). Without that foundation the words used here (pool, Redis, queue, worker) are
just noise. Then come back and read **in the order of the list below** — each block leans on the
previous ones (especially block 02 "Authentication", which is the security foundation for
everything else).

**One-block rule:** one sitting = one block. Don't try to swallow it all. After a block, close the
file and say out loud "what happens here, and where's the hole / why isn't there one." Can't? Read
it again.

---

## The shape of every file

Every block is explained with the same skeleton, so you get used to it:

**Part 1 — teaching (understand the feature):**
1. **Intuition** — the gut-feeling version: why it exists, how it feels.
2. **Detailed walkthrough** — what calls what, every function and key variable named.
3. **Data-flow diagram** — an ASCII map with a legend: who sends what to whom.
4. **Code skeleton** — an empty scaffold so you can mentally fill in the logic yourself.
5. **Heavily commented code** — the real snippets from our code with `file:line` links.
6. **Self-check questions** — 1–2 questions at the end. Answer them before moving on.

**Part 2 — security (the main focus):**
1. **⚙️ Under the hood** — the data path: frontend → HTTP request → backend → third-party APIs / DB → response.
2. **🥷 Threats** — what an attacker could theoretically do to *this specific block* (SQL injection,
   XSS, CSRF, auth-logic bypass, Prompt Injection, payment forgery, token theft, DoS…), with
   concrete scenarios: *where* they'd inject, *which* request they'd forge.
3. **🛡️ Current defenses** — what we *already* have in place (parameterized queries, `initData`
   validation, input filtering, webhook-signature checks, CORS…) and *why* the attack from part 2
   fails because of it.
4. **📈 Recommendations** — what to harden before going to production for hundreds of users (rate
   limiting, extra permission checks, logging suspicious activity).

---

## Menu (clickable)

| # | Block | File | Status |
| --- | --- | --- | --- |
| 00a | **Frontend foundations** — JS vs JSX, function syntax, data types, hooks, `fetch` (read first) | [00a_frontend_foundations.md](00a_frontend_foundations.md) | ✅ |
| 00b | **HTTP + backend foundations** — request anatomy, GET vs POST, status codes, Flask (read second) | [00b_http_and_backend_foundations.md](00b_http_and_backend_foundations.md) | ✅ |
| 01 | **Sentence translation** (frontend ↔ Flask ↔ OpenAI ↔ DB) | [01_sentence_translation.md](01_sentence_translation.md) | ✅ |
| 02 | **Telegram authentication (`initData`)** — the security foundation | [02_telegram_auth_initdata.md](02_telegram_auth_initdata.md) | ✅ |
| 03 | **Payments: Stripe + Telegram Stars** — webhooks, payment forgery | _(planned)_ | ⚪ |
| 04 | **Quick dictionary / word breakdown + Prompt Injection** | _(planned)_ | ⚪ |
| 05 | **Reader / books** | _(planned)_ | ⚪ |
| 06 | **YouTube block (news / video recommendations)** | _(planned)_ | ⚪ |
| 07 | **Text-to-speech (TTS) and budget limits** | _(planned)_ | ⚪ |
| 08 | **Voice / calls (LiveKit)** | _(planned)_ | ⚪ |
| 09 | **Shortcut / iOS Share Extension / auto-save** | _(planned)_ | ⚪ |

> The list grows one block at a time. Finished blocks get a ✅. The order isn't random: the top is
> what you use every day and what's easiest to understand; lower down is infrastructure and
> integrations.

---

*Last checked against the code: 2026-07-18.*
