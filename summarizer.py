/**
 * summarizer.js
 * Pipeline:
 *   1. Translate Korean messages → English  (Google Translate)
 *   2. Deduplicate near-identical stories   (Jaccard similarity)
 *   3. Batch into groups of GROQ_BATCH_SIZE (default 15 / request)
 *   4. Summarise each batch via Groq API    (rate-limited: 30 req/min, 40k tok/min)
 *   5. Render Telegram HTML digest
 */

import Groq                         from "groq-sdk";
import { translateKoreanToEnglish } from "./translator.js";
import { rateLimiter, RateLimiter } from "./rate_limiter.js";
import { GROQ_API_KEY, GROQ_MODEL, GROQ_BATCH_SIZE } from "./config.js";

const groq = new Groq({ apiKey: GROQ_API_KEY });

// ── System prompt ─────────────────────────────────────────────────────────────

const SYSTEM_PROMPT = `\
You are a news editor. You receive a JSON array of translated Korean news messages.
Each item: { "id": number, "text": string }

For EACH item produce:
  - "headline": one concise English headline (≤ 12 words)
  - "summary":  2–3 clear, factual English sentences covering key facts and context

Return ONLY a JSON array — no markdown, no preamble, no extra keys.
Schema: [{ "id": number, "headline": string, "summary": string }, …]`;

// ── Similarity / dedup ────────────────────────────────────────────────────────

function normalise(str) {
  return str.toLowerCase().replace(/\s+/g, " ").trim().slice(0, 80);
}

function jaccardSimilarity(a, b) {
  const A = new Set(a.split(" "));
  const B = new Set(b.split(" "));
  const intersection = [...A].filter((w) => B.has(w)).length;
  const union = new Set([...A, ...B]).size;
  return union === 0 ? 0 : intersection / union;
}

function dedup(items) {
  const kept = [];
  for (const item of items) {
    const norm  = normalise(item.translated);
    const isDup = kept.some((k) => jaccardSimilarity(norm, normalise(k.translated)) > 0.7);
    if (!isDup) kept.push(item);
  }
  return kept;
}

// ── Groq batch call ───────────────────────────────────────────────────────────

/**
 * Summarise one batch of translated messages via Groq.
 * @param {Array<{ id, translated }>} batch
 * @returns {Promise<Map<number, { headline, summary }>>}
 */
async function summariseBatch(batch) {
  const payload = batch.map((item) => ({ id: item.id, text: item.translated }));
  const userMsg = JSON.stringify(payload);

  // Estimate tokens: system prompt + user message + expected output
  const estimatedInput  = RateLimiter.estimateTokens(SYSTEM_PROMPT + userMsg);
  const estimatedOutput = batch.length * 80;   // ~80 output tokens per story
  const estimatedTotal  = estimatedInput + estimatedOutput;

  // Block here if limits are close — rate_limiter handles the sleep
  await rateLimiter.acquire(estimatedTotal);

  console.log(
    `[summarizer] Groq request — ${batch.length} items, ~${estimatedTotal} tokens  ` +
    JSON.stringify(rateLimiter.status())
  );

  const response = await groq.chat.completions.create({
    model:       GROQ_MODEL,
    temperature: 0.3,
    max_tokens:  estimatedOutput + 200,
    messages: [
      { role: "system", content: SYSTEM_PROMPT },
      { role: "user",   content: userMsg },
    ],
  });

  // Correct the limiter with actual token usage
  const actualTokens = response.usage?.total_tokens ?? estimatedTotal;
  rateLimiter.record(actualTokens, estimatedTotal);

  // Parse JSON — strip accidental markdown fences
  let raw = response.choices[0]?.message?.content?.trim() ?? "[]";
  if (raw.startsWith("```")) {
    raw = raw.replace(/^```[a-z]*\n?/, "").replace(/\n?```$/, "");
  }

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    console.error(`[summarizer] JSON parse error: ${err.message}\nRaw:\n${raw}`);
    // Fallback: use raw translated text
    parsed = batch.map((item) => ({
      id:       item.id,
      headline: item.translated.slice(0, 60),
      summary:  item.translated,
    }));
  }

  const map = new Map();
  for (const entry of parsed) map.set(entry.id, entry);
  return map;
}

// ── Main export ───────────────────────────────────────────────────────────────

/**
 * @param {Array<{ channel, date, text, url }>} items
 * @returns {Promise<string>} Telegram HTML-formatted digest
 */
export async function buildDigest(items) {
  // 1. Translate
  console.log(`[summarizer] Translating ${items.length} messages …`);
  const translated = [];
  for (const item of items) {
    const eng = await translateKoreanToEnglish(item.text);
    translated.push({ ...item, translated: eng });
  }

  // 2. Deduplicate
  const unique = dedup(translated);
  console.log(`[summarizer] ${unique.length} unique stories after dedup`);

  // 3. Assign sequential IDs for Groq batch tracking
  const numbered = unique.map((item, i) => ({ ...item, id: i + 1 }));

  // 4. Split into batches and summarise via Groq
  const batches = [];
  for (let i = 0; i < numbered.length; i += GROQ_BATCH_SIZE) {
    batches.push(numbered.slice(i, i + GROQ_BATCH_SIZE));
  }

  console.log(
    `[summarizer] ${batches.length} batch(es) × ≤${GROQ_BATCH_SIZE} items → Groq (${GROQ_MODEL})`
  );

  const summaryMap = new Map();
  for (let b = 0; b < batches.length; b++) {
    console.log(`[summarizer] Batch ${b + 1}/${batches.length} …`);
    const batchMap = await summariseBatch(batches[b]);
    for (const [id, val] of batchMap) summaryMap.set(id, val);
  }

  // 5. Render digest grouped by channel (Telegram HTML)
  const byChannel = {};
  for (const item of numbered) {
    if (!byChannel[item.channel]) byChannel[item.channel] = [];
    byChannel[item.channel].push(item);
  }

  const lines  = [];
  let storyNum = 1;

  for (const [channel, stories] of Object.entries(byChannel)) {
    lines.push(`\n<b>── ${channel} ──</b>`);

    for (const s of stories) {
      const sum      = summaryMap.get(s.id);
      const headline = sum?.headline ?? s.translated.slice(0, 60);
      const summary  = sum?.summary  ?? s.translated;

      lines.push(`<b>${storyNum}. ${headline}</b>`);
      lines.push(summary);
      lines.push(`<a href="${s.url}">🔗 Source</a>  <i>${s.date.slice(0, 10)}</i>`);
      lines.push("");
      storyNum++;
    }
  }

  return lines.join("\n").trim();
}
