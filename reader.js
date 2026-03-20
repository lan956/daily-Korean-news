/**
 * reader.js — fetch recent messages from public Telegram channels via GramJS (MTProto).
 * Uses a StringSession so no interactive login is needed in CI.
 */

import { TelegramClient } from "telegram";
import { StringSession }  from "telegram/sessions/index.js";
import {
  TG_API_ID,
  TG_API_HASH,
  TG_SESSION_STRING,
  LOOKBACK_HOURS,
  MAX_MSGS_PER_CH,
} from "./config.js";

const CUTOFF_MS = LOOKBACK_HOURS * 60 * 60 * 1000;

function buildUrl(channel, msgId) {
  const username = channel.replace(/^@/, "");
  return `https://t.me/${username}/${msgId}`;
}

/**
 * @returns {Promise<Array<{ channel, messageId, date, text, url }>>}
 */
export async function fetchRecentMessages(channel) {
  const session = new StringSession(TG_SESSION_STRING);
  const client  = new TelegramClient(session, TG_API_ID, TG_API_HASH, {
    connectionRetries: 3,
  });

  await client.connect();

  const cutoff = Date.now() - CUTOFF_MS;
  const items  = [];

  const messages = await client.getMessages(channel, { limit: MAX_MSGS_PER_CH });

  for (const msg of messages) {
    const ts = msg.date * 1000; // GramJS uses Unix seconds
    if (ts < cutoff) continue;

    const text = (msg.message || "").trim();
    if (!text || text.length < 20) continue; // skip stickers / empty

    items.push({
      channel,
      messageId: msg.id,
      date:      new Date(ts).toISOString(),
      text,
      url:       buildUrl(channel, msg.id),
    });
  }

  await client.disconnect();

  console.log(`[reader] ${channel}: ${items.length} messages fetched`);
  return items;
}
