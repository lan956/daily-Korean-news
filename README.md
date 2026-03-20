# 🗞️ Korean News Digest Bot

A GitHub Actions-powered Telegram bot that:
1. **Reads** messages from Korean Telegram news channels (via **GramJS** / MTProto)
2. **Translates** them to English (via **@vitalets/google-translate-api** — free, no API key)
3. **Deduplicates** near-identical stories using Jaccard similarity
4. **Sends** a clean numbered digest to your personal Telegram channel

Zero paid APIs required.

---

## Project Structure

```
korean-news-bot/
├── main.js                      # Entry point
├── reader.js                    # GramJS: fetch messages from channels
├── translator.js                # Google Translate wrapper (retry + rate-limit)
├── summarizer.js                # Translate → dedup → Groq summarise → digest
├── rate_limiter.js              # Sliding-window limiter (30 req/min, 40k tok/min)
├── sender.js                    # Telegram Bot API: send digest
├── config.js                    # All settings from env vars
├── package.json
├── .env.example
├── scripts/
│   └── gen_session.js           # One-time helper to generate TG_SESSION_STRING
└── .github/workflows/
    └── daily_digest.yml         # Scheduled GitHub Actions workflow
```

---

## Setup

### 1. Clone & install

```bash
git clone https://github.com/YOUR_USERNAME/korean-news-bot.git
cd korean-news-bot
npm install
```

### 2. Telegram credentials

| Credential | How to get |
|---|---|
| `TG_API_ID` / `TG_API_HASH` | [my.telegram.org/apps](https://my.telegram.org/apps) |
| `TG_SESSION_STRING` | `node scripts/gen_session.js` (run once locally) |
| `BOT_TOKEN` | [@BotFather](https://t.me/BotFather) |
| `TARGET_CHAT_ID` | Your channel username or numeric ID |

> **Important:** The user account behind `TG_SESSION_STRING` must have already **joined** all source channels.

### 3. Groq API key

Get a free key at [console.groq.com](https://console.groq.com). Free tier: **30 req/min, 40 000 tokens/min** — the bot is designed around exactly these limits.

### 4. Configure `.env`

```bash
cp .env.example .env
# fill in all values
```

### 5. GitHub Secrets & Variables

Go to **Settings → Secrets and variables → Actions**.

**Secrets** (sensitive):

| Name | Value |
|---|---|
| `TG_API_ID` | from my.telegram.org |
| `TG_API_HASH` | from my.telegram.org |
| `TG_SESSION_STRING` | output of gen_session.js |
| `BOT_TOKEN` | from @BotFather |
| `TARGET_CHAT_ID` | your channel |
| `GROQ_API_KEY` | from console.groq.com |

**Variables** (freely editable):

| Name | Default | Notes |
|---|---|---|
| `SOURCE_CHANNELS` | `@yonhap_news_kor,@kbs_news,@mbc_news_korea` | comma-separated |
| `LOOKBACK_HOURS` | `24` | |
| `MAX_MSGS_PER_CH` | `30` | |
| `DIGEST_TITLE` | `Korean News Digest` | |
| `GROQ_MODEL` | `llama-3.1-8b-instant` | swap to `llama-3.3-70b-versatile` for better quality |
| `GROQ_BATCH_SIZE` | `15` | messages per Groq request — do not exceed ~20 |

---

## Schedule

Runs every day at **08:00 Hanoi time (01:00 UTC)**. Edit the `cron` line in `.github/workflows/daily_digest.yml` to change it.

Manual trigger: **Actions → Daily Korean News Digest → Run workflow**

---

## Pipeline

```
Korean Telegram channels
      │  GramJS (MTProto)
      ▼
Raw Korean messages
      │  @vitalets/google-translate-api  (free, no key)
      ▼
Translated English messages
      │  Jaccard dedup  (drops >70% similar stories)
      ▼
Unique stories  →  batched at 15/request
      │  Groq API  (rate-limited: 30 req/min, 40k tok/min)
      ▼
{headline, summary} per story
      │  Telegram Bot API
      ▼
Your personal channel
```

| Username | Outlet |
|---|---|
| `@yonhap_news_kor` | Yonhap News Agency |
| `@kbs_news` | KBS News |
| `@mbc_news_korea` | MBC News |
| `@jtbcnews` | JTBC News |
| `@chosunilbo_news` | Chosun Ilbo |

---

## Local Test

```bash
node main.js
```

---

## License

MIT
