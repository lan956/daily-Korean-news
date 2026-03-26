/**
 * translator.js — wraps @vitalets/google-translate-api.
 * Handles per-request errors, retries, and a polite delay to avoid 429s.
 */

import translate from "@vitalets/google-translate-api";

const DELAY_MS      = 300;  // ms between requests — be polite to Google
const MAX_RETRIES   = 3;
const RETRY_DELAY   = 2000;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Translate a single Korean string to English.
 * @param {string} text
 * @returns {Promise<string>}
 */
export async function translateKoreanToEnglish(text) {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const result = await translate(text, { from: "ko", to: "en" });
      await sleep(DELAY_MS);
      return result.text;
    } catch (err) {
      const isRateLimit = err?.message?.includes("429") || err?.message?.includes("Too Many");
      console.warn(`[translator] attempt ${attempt} failed: ${err.message}`);
      if (attempt < MAX_RETRIES) {
        await sleep(isRateLimit ? RETRY_DELAY * attempt : RETRY_DELAY);
      } else {
        console.error("[translator] All retries exhausted — returning original text.");
        return text; // fallback: return untranslated
      }
    }
  }
}
