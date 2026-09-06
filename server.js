// dictionary-server: the HTTP API the degoog define-slot plugin calls.
//
// Serves a kaikki word entry from the SQLite database built by import.js /
// refresh.js, plus a pronunciation-audio resolver.
//
//   GET /health                      -> {status:"ok", words}
//   GET /api/en/{word}               -> the English entry for a word (404 if absent)
//   GET /audio/{word}?accent=uk      -> pronunciation audio (see below)
//
// HOT-SWAP: when refresh.js finishes building a new staging database it writes
// /data/.refresh-ready (after producing /data/wiktionary.db.new). The server
// polls for that marker every few seconds; when it appears it closes the current
// SQLite handle, moves wiktionary.db.new over wiktionary.db, removes the marker,
// and reopens. Lookups on the old data keep serving until the instant of the
// move, so a refresh never interrupts the dictionary.
//
// AUDIO RESOLUTION: GET /audio/{word}[?accent=uk|us] walks a source chain and
// caches results to disk:
//   1. cached real file        -> serve from disk (ms, zero egress)
//   2. Cambridge Dictionary    -> UK/US mp3 from the page (professional recordings)
//   3. Wiktionary (Wikimedia)  -> the DB's stored sounds.mp3_url (real human)
//   4. Google translate_tts    -> synthetic, short TTL
//   5. fail-clean 404
// All external fetches go through HTTPS_PROXY (Proton egress) when set; the
// browser only ever talks to this local server. Cache files are tagged by source in the
// filename so real recordings always win over synthetic TTS, and TTS files
// expire (~30 days) so they do not linger once a real recording exists.

import { Database } from "bun:sqlite";
import { join } from "node:path";
import { existsSync, renameSync, statSync, readdirSync, rmSync, mkdirSync } from "node:fs";
import { createHash } from "node:crypto";

const PORT = Number(process.env.PORT || 3000);
const DATA_DIR = process.env.DATA_DIR || "./data";
const DB_PATH = join(DATA_DIR, "wiktionary.db");
const NEW_DB_PATH = join(DATA_DIR, "wiktionary.db.new");
const READY_MARKER = join(DATA_DIR, ".refresh-ready");
const AUDIO_DIR = join(DATA_DIR, "audio");

const SWAP_POLL_MS = Number(process.env.SWAP_POLL_MS || 5000);
const AUDIO_TTL_MS = 30 * 24 * 60 * 60 * 1000; // TTS cache expiry: 30 days
const FETCH_TIMEOUT_MS = 2500; // per-tier deadline

let db;
let SELECT_ENTRIES;
let COUNT_WORDS;
let SELECT_SOUNDS;
let WORD_EXISTS;

function openDb() {
  const conn = new Database(DB_PATH, { readonly: true });
  // Keep the deck warm: cache SQLite pages in memory so every lookup is fast.
  conn.exec("PRAGMA cache_size = -65536"); // 64 MB
  conn.exec("PRAGMA mmap_size = 268435456"); // 256 MB
  return conn;
}

function prepareStatements(conn) {
  SELECT_ENTRIES = conn.prepare(`
    SELECT lang_code, pos, senses, sounds, etymology, related
    FROM entries
    WHERE word = ? AND lang_code = ?
    ORDER BY pos
  `);
  SELECT_SOUNDS = conn.prepare(`
    SELECT sounds FROM entries
    WHERE word = ? AND lang_code = ? AND sounds IS NOT NULL
  `);
  WORD_EXISTS = conn.prepare(`
    SELECT 1 FROM entries WHERE word = ? AND lang_code = ? LIMIT 1
  `);
  COUNT_WORDS = conn.prepare(`SELECT COUNT(*) AS count FROM entries`);
}

// Try to adopt a freshly-built staging database. Returns true if a swap happened.
function trySwap() {
  const haveLive = db != null;
  if (!existsSync(READY_MARKER)) return false;
  if (!existsSync(NEW_DB_PATH)) {
    rmSync(READY_MARKER, { force: true });
    return false;
  }

  if (haveLive) {
    console.log("hot-swap: adopting new staging database");
    db.close();
    renameSync(NEW_DB_PATH, DB_PATH);
    rmSync(READY_MARKER, { force: true });
  } else {
    console.log("initial load: adopting staging database as wiktionary.db");
    renameSync(NEW_DB_PATH, DB_PATH);
    rmSync(READY_MARKER, { force: true });
  }

  db = openDb();
  prepareStatements(db);
  console.log("database ready");
  return true;
}

db = null;
try {
  if (existsSync(DB_PATH)) {
    db = openDb();
    prepareStatements(db);
    console.log(`database ready: ${DB_PATH}`);
  } else if (!trySwap()) {
    console.log(`no database yet at ${DB_PATH}; waiting for a refresh to produce one`);
  }
} catch (err) {
  console.error("initial load failed:", err);
}

setInterval(() => {
  try {
    trySwap();
  } catch (err) {
    console.error("hot-swap failed:", err);
  }
}, SWAP_POLL_MS);

function lookup(word, langCode) {
  if (!db) return null;
  const rows = SELECT_ENTRIES.all(word, langCode);
  if (!rows.length) return null;
  return {
    word,
    editions: ["en"],
    entries: rows.map((row) => ({
      lang_code: row.lang_code,
      pos: row.pos,
      senses: JSON.parse(row.senses || "[]"),
      sounds: JSON.parse(row.sounds || "[]"),
      etymology: row.etymology || "",
      related: JSON.parse(row.related || "{}"),
    })),
  };
}

// ---------------------------------------------------------------------------
// Audio: cache + source chain
// ---------------------------------------------------------------------------

function sha1(input) {
  return createHash("sha1").update(input).digest("hex").slice(0, 16);
}

// Cache file name encodes (word, accent, source) so Cambridge/Wiktionary/TTS
// copies of the same word never collide. Real sources are "real:...", TTS be
// "tts:..." (short TTL).
function audioCachePath(word, accent, source) {
  const key = `${word}|${accent}|${source}`;
  return join(AUDIO_DIR, `${sha1(key)}-${source}.mp3`);
}

function fetchWithTimeout(url, init = {}) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  return fetch(url, { ...init, signal: ctrl.signal })
    .finally(() => clearTimeout(timer));
}

function stripTags(html) {
  return String(html || "").replace(/<[^>]+>/g, "");
}

// Serve a cached audio file directly from disk. Returns a Response or null.
function serveCached(path) {
  if (!existsSync(path)) return null;
  const st = statSync(path);
  if (st.size === 0) return null;
  // TTS cache entries expire; real entries never do.
  if (path.includes("-tts") && Date.now() - st.mtimeMs > AUDIO_TTL_MS) {
    rmSync(path, { force: true });
    return null;
  }
  return new Response(Bun.file(path), {
    headers: { "Content-Type": "audio/mpeg", "Cache-Control": "public, max-age=86400" },
  });
}

function saveAudio(path, bytes) {
  mkdirSync(AUDIO_DIR, { recursive: true });
  // Write atomically: temp file then rename, so a crashed write never leaves a
  // half-file that we'd then serve back.
  const tmp = `${path}.tmp`;
  Bun.write(tmp, bytes);
  renameSync(tmp, path);
}

// Tier 2: Cambridge Dictionary. Returns downloaded bytes or null.
async function cambridgeAudio(word, accent) {
  const lemma = encodeURIComponent(word);
  const pageUrl = `https://dictionary.cambridge.org/dictionary/english/${lemma}`;
  const res = await fetchWithTimeout(pageUrl, {
    headers: { "User-Agent": "degoog-dictionary/1.0 (self-hosted dictionary card)" },
  });
  if (!res.ok) return null;
  const html = await res.text();
  const kind = accent === "us" ? "us_pron" : "uk_pron";
  // Match either absolute or relative media URLs for the requested accent.
  const re = new RegExp(
    `https://dictionary\\.cambridge\\.org/media/english/${kind}/[^"']+\\.mp3`,
    "i",
  );
  let m = html.match(re);
  if (!m) {
    // Relative path form: /media/english/{kind}/...
    const rel = html.match(new RegExp(`(/media/english/${kind}/[^"']+\\.mp3)`, "i"));
    if (rel) m = rel;
    else return null;
  }
  let src = m[1];
  if (src.startsWith("/")) src = `https://dictionary.cambridge.org${src}`;
  const audio = await fetchWithTimeout(src, {
    headers: { "User-Agent": "degoog-dictionary/1.0 (self-hosted dictionary card)", Accept: "audio/*" },
  });
  if (!audio.ok) return null;
  const bytes = await audio.arrayBuffer();
  return bytes.byteLength ? new Uint8Array(bytes) : null;
}

// Tier 3: Wiktionary / Wikimedia - pick a stored clip for the word, preferring
// a clip that matches the requested accent when the filename says so.
function wiktionarySound(word, accent) {
  if (!db) return null;
  const rows = SELECT_SOUNDS.all(word, "en");
  if (!rows.length) return null;
  let candidates = [];
  for (const row of rows) {
    for (const snd of JSON.parse(row.sounds || "[]")) {
      for (const k of ["mp3_url", "ogg_url"]) {
        if (snd[k]) candidates.push(String(snd[k]));
      }
    }
  }
  if (!candidates.length) return null;
  const want = accent === "us" ? "en-us" : accent === "uk" ? /en-(uk|gb)?[-.]/i : null;
  if (want) {
    const match = candidates.find((u) => new RegExp(want, "i").test(u));
    if (match) return match;
  }
  return candidates[0];
}

async function wiktionaryAudio(word, accent) {
  const url = wiktionarySound(word, accent);
  if (!url) return null;
  const res = await fetchWithTimeout(url, {
    headers: { "User-Agent": "degoog-dictionary/1.0 (self-hosted dictionary card)", Accept: "audio/*" },
  });
  if (!res.ok) return null;
  const bytes = await res.arrayBuffer();
  return bytes.byteLength ? new Uint8Array(bytes) : null;
}

// Tier 4: Google translate_tts - synthetic, any word, specified accent.
async function ttsAudio(word, accent) {
  const tl = accent === "us" ? "en-US" : "en-GB";
  const url = `https://translate.google.com/translate_tts?ie=UTF-8&q=${encodeURIComponent(word)}&tl=${tl}&client=tw-ob`;
  const res = await fetchWithTimeout(url, {
    headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)" },
  });
  if (!res.ok) return null;
  const bytes = await res.arrayBuffer();
  return bytes.byteLength ? new Uint8Array(bytes) : null;
}

async function resolveAudio(word, accent) {
  // 1. Cached real file (Cambridge or Wiktionary) wins outright.
  for (const src of ["cambridge", "wiktionary"]) {
    const hit = serveCached(audioCachePath(word, accent, src));
    if (hit) return hit;
  }

  // 2. Cambridge (professional, UK+US).
  try {
    const bytes = await cambridgeAudio(word, accent);
    if (bytes) {
      const path = audioCachePath(word, accent, "cambridge");
      saveAudio(path, bytes);
      return serveCached(path);
    }
  } catch (err) {
    console.error(`audio cambridge failed for ${word}:`, err?.message || err);
  }

  // 3. Wiktionary / Wikimedia (real human, whatever clip exists).
  try {
    const bytes = await wiktionaryAudio(word, accent);
    if (bytes) {
      const path = audioCachePath(word, accent, "wiktionary");
      saveAudio(path, bytes);
      return serveCached(path);
    }
  } catch (err) {
    console.error(`audio wiktionary failed for ${word}:`, err?.message || err);
  }

  // 4. Google TTS (synthetic, short TTL) - caches as tts, expires in 30 days.
  const ttsPath = audioCachePath(word, accent, "tts");
  let hit = serveCached(ttsPath);
  if (hit) return hit;
  try {
    const bytes = await ttsAudio(word, accent);
    if (bytes) {
      saveAudio(ttsPath, bytes);
      return serveCached(ttsPath);
    }
  } catch (err) {
    console.error(`audio tts failed for ${word}:`, err?.message || err);
  }

  // 5. Fail clean: explicit 404 the plugin can render as "no audio".
  return Response.json({ error: "No audio available" }, { status: 404 });
}

Bun.serve({
  port: PORT,
  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      if (!db) return Response.json({ status: "loading", words: 0 });
      return Response.json({ status: "ok", words: COUNT_WORDS.get().count });
    }

    // /api/en/{word}
    const apiMatch = url.pathname.match(/^\/api\/en\/(.+)$/);
    if (apiMatch) {
      const word = decodeURIComponent(apiMatch[1]).toLowerCase();
      const lang = url.searchParams.get("lang") || "en";
      const result = lookup(word, lang);
      if (!result) {
        return Response.json({ error: `No entries found for "${word}"` }, { status: 404 });
      }
      return Response.json(result);
    }

    // /audio/{word}?accent=uk|us
    const audioMatch = url.pathname.match(/^\/audio\/(.+)$/);
    if (audioMatch) {
      const word = decodeURIComponent(audioMatch[1]).toLowerCase();
      const accent = url.searchParams.get("accent") === "us" ? "us" : "uk";
      // Only real dictionary headwords qualify for pronunciation audio. A word
      // with no entry at all (nonsense/typo/random string) must never reach the
      // external sources or pollute the cache with TTS noise.
      if (!db || !WORD_EXISTS.get(word, "en")) {
        return Response.json({ error: "No audio available" }, { status: 404 });
      }
      return resolveAudio(word, accent);
    }

    return Response.json({ error: "not found" }, { status: 404 });
  },
});

console.log(`dictionary-server listening on :${PORT}, db ${DB_PATH}`);