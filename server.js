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
// Opt-in last-resort definition source. The local DB is a weekly, English-only
// cut of Wiktionary; a word absent from it (foreign term, or one added since the
// last extract) normally renders an empty card. When this is on (default), a
// local miss fires one bounded fetch to Wiktionary's live REST API as an
// alternate definition source. Disable it (WIKTIONARY_FALLBACK=0) to keep the
// instance strictly self-hosted - no egress at all. External fetches honour
// HTTPS_PROXY (Proton exit) like the audio tiers do.
const WIKTIONARY_FALLBACK = String(process.env.WIKTIONARY_FALLBACK || "1") !== "0";
const WIKTIONARY_REST = (lang) =>
  `https://en.wiktionary.org/api/rest_v1/page/definition/${lang}`;

// Map a Wiktionary REST definition payload into the same entry shape the local
// DB server and the define-slot plugin already understand. The requested
// language (English for a normal lookup) is preferred first; when it is absent
// - a foreign word with no English sense - the other language buckets are
// served so the card still shows *something* (the word's definition in its own
// language) rather than an empty card. Each entry carries its real lang_code.
// Definitions come back as tiny HTML (links, usage spans) which is stripped to
// plain text. Returns null when nothing usable came back.
function mapRESTResponse(payload, langCode) {
  if (!payload || typeof payload !== "object") return null;

  // Order buckets: the requested language first, then everything else. Keep
  // bucket order stable (entries sorted by language) so output is deterministic.
  const keys = Object.keys(payload);
  const ordered = [...keys].sort((a, b) => {
    const ap = a === langCode ? 0 : 1;
    const bp = b === langCode ? 0 : 1;
    return ap !== bp ? ap - bp : a.localeCompare(b);
  });

  const entries = [];
  for (const lang of ordered) {
    const bucket = payload[lang];
    if (!Array.isArray(bucket)) continue;
    for (const block of bucket) {
      if (typeof block !== "object" || !block) continue;
      const defs = Array.isArray(block.definitions) ? block.definitions : [];
      const glosses = defs
        .map((d) => stripTags(String(d?.definition || "")))
        .map((s) => s.trim())
        .filter((s) => s.length > 0);
      if (!glosses.length) continue;
      entries.push({
        lang_code: lang,
        pos: String(block.partOfSpeech || "").trim(),
        senses: [{ glosses }],
        sounds: [],
        etymology: "",
        related: {},
      });
    }
  }

  return entries.length ? entries : null;
}

// Wiktionary REST fallback for a local miss. Returns a server entry object, or
// null if the live API has nothing / errors / times out (the caller keeps the
// fail-clean 404). Bounded by FETCH_TIMEOUT_MS so a slow Wiktionary never holds
// the card open past its budget.
async function wiktionaryRest(word, langCode) {
  if (!WIKTIONARY_FALLBACK) return null;
  const url = WIKTIONARY_REST(encodeURIComponent(word));
  try {
    const res = await fetchWithTimeout(url, {
      headers: { "User-Agent": "degoog-dictionary/1.0 (self-hosted dictionary card)" },
    });
    if (!res.ok) return null;
    const payload = await res.json();
    const entries = mapRESTResponse(payload, langCode);
    if (!entries) return null;
    return {
      word,
      editions: ["en"],
      source: "wiktionary-rest",
      entries,
    };
  } catch {
    return null;
  }
}

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

  // Cambridge has no entry page for some words: it 30x-redirects to the bare
  // /dictionary/english/ (or a suggestion page), which still contains audio
  // blocks for OTHER words. The first uk_pron/us_pron mp3 there would be
  // someone else's clip - literally "shellfish" for "lycanthropic". A real
  // entry keeps the final URL pointed at the requested word; anything else is
  // a clean miss (we fall through to Wiktionary/TTS), never a wrong-word clip.
  const finalUrl = String(res.url || "");
  const expected = `/dictionary/english/${lemma}`;
  if (!finalUrl.toLowerCase().includes(expected.toLowerCase())) return null;

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
  const t0 = Date.now();
  // steps: per-tier outcome log. Each records {step, outcome, ms, detail}.
  const steps = [];
  const trace = (step, outcome, ms, detail = "") =>
    steps.push({ step, outcome, ms: Math.round(ms), detail });

  const elapsed = () => Date.now() - t0;

  // 1. Cached real file (Cambridge or Wiktionary) wins outright.
  for (const src of ["cambridge", "wiktionary"]) {
    const bytesPath = audioCachePath(word, accent, src);
    const hit = serveCached(bytesPath);
    if (hit) {
      trace(src, "cache-hit", elapsed(), Math.round(statSync(bytesPath).size / 1024) + "KB");
      logAudio(word, accent, steps, elapsed(), src, true);
      return hit;
    }
    trace(src, "cache-miss", elapsed());
  }

  // 2. Cambridge (professional, UK+US).
  const c0 = Date.now();
  let bytes = null;
  let errMsg = "";
  try {
    bytes = await cambridgeAudio(word, accent);
  } catch (err) {
    errMsg = String(err?.message || err).slice(0, 80);
  }
  if (bytes) {
    const t = Date.now() - c0;
    const bytesPath = audioCachePath(word, accent, "cambridge");
    saveAudio(bytesPath, bytes);
    trace("cambridge", "fetch-ok", t, Math.round(bytes.byteLength / 1024) + "KB");
    trace("cambridge", "cache-store", elapsed());
    logAudio(word, accent, steps, elapsed(), "cambridge", false);
    return serveCached(bytesPath);
  }
  trace("cambridge", errMsg ? "error:" + errMsg : "no-entry", Date.now() - c0);

  // 3. Wiktionary / Wikimedia (real human, whatever clip exists).
  const w0 = Date.now();
  bytes = null;
  errMsg = "";
  try {
    bytes = await wiktionaryAudio(word, accent);
  } catch (err) {
    errMsg = String(err?.message || err).slice(0, 80);
  }
  if (bytes) {
    const t = Date.now() - w0;
    const bytesPath = audioCachePath(word, accent, "wiktionary");
    saveAudio(bytesPath, bytes);
    trace("wiktionary", "fetch-ok", t, Math.round(bytes.byteLength / 1024) + "KB");
    trace("wiktionary", "cache-store", elapsed());
    logAudio(word, accent, steps, elapsed(), "wiktionary", false);
    return serveCached(bytesPath);
  }
  trace("wiktionary", errMsg ? "error:" + errMsg : "no-entry", Date.now() - w0);

  // 4. Google TTS (synthetic, short TTL expires ~30 days).
  const ttsPath = audioCachePath(word, accent, "tts");
  let hit = serveCached(ttsPath);
  if (hit) {
    trace("tts", "cache-hit", elapsed(), Math.round(statSync(ttsPath).size / 1024) + "KB");
    logAudio(word, accent, steps, elapsed(), "tts", true);
    return hit;
  }
  trace("tts", "cache-miss", elapsed());

  const tt0 = Date.now();
  bytes = null;
  errMsg = "";
  try {
    bytes = await ttsAudio(word, accent);
  } catch (err) {
    errMsg = String(err?.message || err).slice(0, 80);
  }
  if (bytes) {
    const t = Date.now() - tt0;
    saveAudio(ttsPath, bytes);
    trace("tts", "fetch-ok", t, Math.round(bytes.byteLength / 1024) + "KB");
    trace("tts", "cache-store", elapsed());
    logAudio(word, accent, steps, elapsed(), "tts", false);
    return serveCached(ttsPath);
  }
  trace("tts", errMsg ? "error:" + errMsg : "no-entry", Date.now() - tt0);

  // 5. Fail clean: explicit 404 the plugin can render as "no audio".
  logAudio(word, accent, steps, elapsed(), "none", false);
  return Response.json({ error: "No audio available" }, { status: 404 });
}

// Emit one structured line per resolution for Dozzle/grep friendliness.
function logAudio(word, accent, steps, totalMs, source, cacheHit) {
  const walk = steps
    .map((s) => {
      let base = s.step + "=" + s.outcome;
      if (s.ms != null) base += "(" + s.ms + "ms)";
      if (s.detail) base += ":" + s.detail;
      return base;
    })
    .join(" ");
  console.log(
    "audio word=" + word + " accent=" + accent + " src=" + source + " cache=" + (cacheHit ? "hit" : "miss") + " total=" + totalMs + "ms " + walk,
  );
}

Bun.serve({
  port: PORT,
  async fetch(request) {
    const t0 = Date.now();
    const url = new URL(request.url);
    const log = (status, extra = "") =>
      console.log(
        `req method=${request.method} path=${url.pathname} status=${status} ms=${Date.now() - t0}${extra ? " " + extra : ""}`,
      );

    if (url.pathname === "/health") {
      if (!db) return Response.json({ status: "loading", words: 0 });
      const res = Response.json({ status: "ok", words: COUNT_WORDS.get().count });
      log(200);
      return res;
    }

    // /api/en/{word}
    const apiMatch = url.pathname.match(/^\/api\/en\/(.+)$/);
    if (apiMatch) {
      const word = decodeURIComponent(apiMatch[1]).toLowerCase();
      const lang = url.searchParams.get("lang") || "en";
      const result = lookup(word, lang);
      if (!result) {
        // Local miss: never fail clean until the opt-in Wiktionary REST
        // fallback has had its one bounded call. If that finds nothing,
        // error, or is disabled, the 404 stands.
        const fallback = await wiktionaryRest(word, lang);
        if (fallback) {
          log(200, `word=${word} entries=${fallback.entries.length} source=wiktionary-rest`);
          return Response.json(fallback);
        }
        log(404, `word=${word}`);
        return Response.json({ error: `No entries found for "${word}"` }, { status: 404 });
      }
      log(200, `word=${word} entries=${result.entries.length}`);
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
        log(404, `audio word=${word} accent=${accent} blocked=not-in-db`);
        return Response.json({ error: "No audio available" }, { status: 404 });
      }
      // resolveAudio logs its own detailed trace line, so the req line here is
      // kept light (opaque: we do not know the outcome until the call resolves).
      const res = await resolveAudio(word, accent);
      log(res.status, `audio word=${word} accent=${accent}`);
      return res;
    }

    log(404);
    return Response.json({ error: "not found" }, { status: 404 });
  },
});

console.log(`dictionary-server listening on :${PORT}, db ${DB_PATH}`);