/**
 * rate_limiter.js
 * Sliding-window rate limiter for Groq's API constraints:
 *   - 30 requests / minute
 *   - 40 000 tokens  / minute
 *
 * Call `await rateLimiter.acquire(estimatedTokens)` before every Groq request.
 * Call `rateLimiter.record(actualTokens)` after the response arrives.
 */

const WINDOW_MS    = 60_000;
const MAX_REQUESTS = 28;       // keep 2 in reserve
const MAX_TOKENS   = 38_000;   // keep 2 000 in reserve

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

class RateLimiter {
  constructor() {
    this._windowStart = Date.now();
    this._requests    = 0;
    this._tokens      = 0;
  }

  /** Rough token estimate: 1 token ≈ 4 characters */
  static estimateTokens(text) {
    return Math.ceil(text.length / 4);
  }

  _resetIfExpired() {
    if (Date.now() - this._windowStart >= WINDOW_MS) {
      this._windowStart = Date.now();
      this._requests    = 0;
      this._tokens      = 0;
    }
  }

  /**
   * Block until there is capacity for `estimatedTokens` tokens + 1 request.
   * @param {number} estimatedTokens
   */
  async acquire(estimatedTokens) {
    this._resetIfExpired();

    const requestsOk = this._requests < MAX_REQUESTS;
    const tokensOk   = this._tokens + estimatedTokens < MAX_TOKENS;

    if (!requestsOk || !tokensOk) {
      const elapsed = Date.now() - this._windowStart;
      const waitMs  = WINDOW_MS - elapsed + 500; // +500 ms safety margin
      const reason  = !requestsOk ? "request cap" : "token cap";
      console.log(
        `[rate_limiter] ${reason} reached (reqs=${this._requests}, tokens≈${this._tokens}) — ` +
        `waiting ${Math.ceil(waitMs / 1000)}s for window reset …`
      );
      await sleep(waitMs);
      this._windowStart = Date.now();
      this._requests    = 0;
      this._tokens      = 0;
    }

    // Reserve the slot optimistically
    this._requests += 1;
    this._tokens   += estimatedTokens;
  }

  /**
   * Update token count with the actual value returned by the API.
   * @param {number} actualTokens
   * @param {number} estimatedTokens  — the value passed to acquire()
   */
  record(actualTokens, estimatedTokens) {
    this._tokens += actualTokens - estimatedTokens; // correct the estimate
  }

  status() {
    this._resetIfExpired();
    return {
      requests: this._requests,
      tokens:   this._tokens,
      windowAgeMs: Date.now() - this._windowStart,
    };
  }
}

export const rateLimiter = new RateLimiter();
