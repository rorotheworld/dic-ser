// dictionary-server: the HTTP API the degoog define-slot plugin calls.
//
// Serves a kaikki word entry from the SQLite database built by import.js /
// refresh.js. The plugin does the rendering; this endpoint just returns the
// entry as JSON, which keeps the server a dumb data appliance.
//
//   GET /health                      -> {status:"ok", words}
//   GET /api/en/{word}               -> the English entry for a word (404 if absent)
//
// HOT-SWAP: when refresh.js finishes building a new staging database it writes
// /data/.refresh-ready (after producing /data/wiktionary.db.new). The server
// polls for that marker every few seconds; when it appears it closes the current
// SQLite handle, moves wiktionary.db.new over wiktionary.db, removes the marker,
// and reopens. Lookups on the old data keep serving until the instant of the
// move, so a refresh never interrupts the dictionary.

import { Database } from "bun:sqlite";
import { join } from "node:path";
import { existsSync, renameSync } from "node:fs";
import { rmSync } from "node:fs";

const PORT = Number(process.env.PORT || 3000);
const DATA_DIR = process.env.DATA_DIR || "./data";
const DB_PATH = join(DATA_DIR, "wiktionary.db");
const NEW_DB_PATH = join(DATA_DIR, "wiktionary.db.new");
const READY_MARKER = join(DATA_DIR, ".refresh-ready");

const SWAP_POLL_MS = Number(process.env.SWAP_POLL_MS || 5000);

let db;
let SELECT_ENTRIES;
let COUNT_WORDS;

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
  COUNT_WORDS = conn.prepare(`SELECT COUNT(*) AS count FROM entries`);
}

// Try to adopt a freshly-built staging database. Returns true if a swap happened.
// Also serves as the initial load when no live DB exists yet: if there is no
// wiktionary.db but a finished staging file plus marker are present, that staging
// file becomes the live DB.
function trySwap() {
  const haveLive = db != null;
  // A swap (initial adopt or hot-swap) is only legitimate when the build
  // finished and dropped the marker. A marker-less staging file can be half-
  // built (a hard crash skips refresh.js's cleanup), so requiring the marker
  // stops a first-boot from adopting a corrupt DB.
  if (!existsSync(READY_MARKER)) return false;
  if (!existsSync(NEW_DB_PATH)) {
    // Marker without a staging file: a refresh that died between writing the
    // marker and the move. Drop the marker so we don't retry against nothing.
    rmSync(READY_MARKER, { force: true });
    return false;
  }

  if (haveLive) {
    console.log("hot-swap: adopting new staging database");
    db.close();
    // Move the completed staging file over the live one (atomic on the same fs).
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

// Boot: if a live DB exists, use it; otherwise wait for the poll loop to adopt a
// staging file. Never crash-loop on a missing database - a wipe is a recovery
// path, not a failure (2026-09-05 regression fixed).
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

// Poll for a refresh-ready marker (or a first database) in the background.
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

Bun.serve({
  port: PORT,
  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      if (!db) {
        return Response.json({ status: "loading", words: 0 });
      }
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

    return Response.json({ error: "not found" }, { status: 404 });
  },
});

console.log(`dictionary-server listening on :${PORT}, db ${DB_PATH}`);