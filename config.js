/**
 * config.js — all settings from environment variables.
 * Secrets go in .env (local) or GitHub Secrets (CI). Never hard-code them.
 */

import "dotenv/config";

function require_env(name) {
  const val = process.env[name];
  if (!val) throw new Error(`Missing required env var: ${name}`);
  return val;
}

// ── Telegram MTProto (GramJS) — for READING channels ─────────────────────────
export const TG_API_ID        = parseInt(require_env("TG_API_ID"), 10);
export const TG_API_HASH      = require_env("TG_API_HASH");
export const TG_SESSION_STRING = require_env("TG_SESSION_STRING");

// ── Telegram Bot API — for SENDING the digest ─────────────────────────────────
export const BOT_TOKEN      = require_env("BOT_TOKEN");
export const TARGET_CHAT_ID = require_env("TARGET_CHAT_ID");

// ── Source channels ───────────────────────────────────────────────────────────
export const CHANNELS = (
  process.env.SOURCE_CHANNELS || "@yonhap_news_kor,@kbs_news,@mbc_news_korea"
)
  .split(",")
  .map((c) => c.trim())
  .filter(Boolean);

// ── Groq ──────────────────────────────────────────────────────────────────────
export const GROQ_API_KEY   = require_env("GROQ_API_KEY");
// llama-3.1-8b-instant: fast + generous rate limits
// llama-3.3-70b-versatile: higher quality but lower rate limits
export const GROQ_MODEL     = process.env.GROQ_MODEL || "llama-3.1-8b-instant";
// Messages per Groq request — keep at 15 to stay within 40k tok/min
export const GROQ_BATCH_SIZE = parseInt(process.env.GROQ_BATCH_SIZE || "15", 10);

// ── Behaviour ─────────────────────────────────────────────────────────────────
export const LOOKBACK_HOURS  = parseInt(process.env.LOOKBACK_HOURS  || "24", 10);
export const MAX_MSGS_PER_CH = parseInt(process.env.MAX_MSGS_PER_CH || "30", 10);
export const DIGEST_TITLE    = process.env.DIGEST_TITLE || "Korean News Digest";
