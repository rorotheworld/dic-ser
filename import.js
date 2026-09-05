// dictionary-server: import of kaikki.org wiktextract data into SQLite.
//
// Reads a JSON-Lines file produced by wiktextract (the same data dictionaryapi.dev
// and wiktapi are built on) and loads it into a staging SQLite database.
//
// Two reductions intentional, decided 2026-09-05:
//   1. LANGUAGE FILTER - by default only `lang_code: "en"` (actual English
//      words) is imported. The raw English-Wiktionary extract is 10.8M entries
//      of which only ~1.54M (14%) are English words; the rest are foreign words
//      glossed in English. The mechanism earlier crash: the full multilingual
//      import committed ~25 GB and Bun died on this 8.33 GB host. Set
//      INCLUDE_LANGS to "all" (or a comma list) to import the multilingual data.
//   2. FIELD PROJECTION - only what the degoog define-slot card renders is stored:
//      glosses, example text, synonym/antonym names, ipa + audio URLs, etymology
//      text, and the related-word lists. Raw sense objects are huge (a single
//      133 KB line for "dictionary"); everything the card does not show is
//      dropped at import, keeping the DB small and lookups fast.
//
// The database is written to a STAGING path and must be swapped into place by the
// server's hot-swap (see server.js) or refresh.js. This file never touches the
// live DB.

import { Database } from "bun:sqlite";
import { createReadStream } from "node:fs";
import { statSync } from "node:fs";
import { createInterface } from "node:readline";

// Which lang_codes to import. Env override; default English words only.
const INCLUDED_LANGS = (process.env.INCLUDE_LANGS || "en")
  .split(",")
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);

// Compact schema: channels exactly the card's fields.
const TABLE_DDL = `
  CREATE TABLE IF NOT EXISTS entries (
    id        INTEGER PRIMARY KEY,
    word      TEXT    NOT NULL,
    lang_code TEXT    NOT NULL,
    pos       TEXT,
    senses    TEXT    NOT NULL,  -- JSON [{glosses[], examples[], synonyms[], antonyms[]}]
    sounds    TEXT,              -- JSON [{ipa, ogg_url, mp3_url}]
    etymology TEXT,              -- trimmed etymology_text
    related   TEXT               -- JSON {derived[], related[], synonyms[], antonyms[]}
  );
`;

const INDEX_DDL = `
  CREATE INDEX IF NOT EXISTS idx_word_lang ON entries (word, lang_code);
`;

const INSERT_SQL = `
  INSERT INTO entries (word, lang_code, pos, senses, sounds, etymology, related)
  VALUES ($word, $lang_code, $pos, $senses, $sounds, $etymology, $related)
`;

const BATCH_SIZE = 20_000;

// wiktextract sometimes lists related words as plain strings, sometimes as
// objects with .word or .name. Accept both, dedupe, keep order.
function linkWords(list) {
  const out = [];
  for (const item of Array.isArray(list) ? list : []) {
    const name = typeof item === "string" ? item : item?.word || item?.name;
    if (name && typeof name === "string" && !out.includes(name)) out.push(name);
  }
  return out;
}

// Reduce one raw entry to the compact shape the card renders.
function compactEntry(parsed) {
  const senses = (Array.isArray(parsed.senses) ? parsed.senses : []).map((s) => {
    if (!s || typeof s !== "object") {
      return { glosses: [], examples: [], synonyms: [], antonyms: [] };
    }
    const describe = (x) => (typeof x === "string" ? x : x?.text || x?.example || "");
    return {
      glosses: Array.isArray(s.glosses) ? s.glosses.filter((g) => typeof g === "string") : [],
      examples: (Array.isArray(s.examples) ? s.examples : []).map(describe).filter(Boolean),
      synonyms: linkWords(s.synonyms),
      antonyms: linkWords(s.antonyms),
    };
  });

  const sounds = (Array.isArray(parsed.sounds) ? parsed.sounds : [])
    .map((snd) => ({
      ipa: typeof snd?.ipa === "string" ? snd.ipa : undefined,
      ogg_url: typeof snd?.ogg_url === "string" ? snd.ogg_url : undefined,
      mp3_url: typeof snd?.mp3_url === "string" ? snd.mp3_url : undefined,
    }))
    .filter((s) => s.ipa || s.ogg_url || s.mp3_url);

  const rel = {
    derived: linkWords(parsed.derived),
    related: linkWords(parsed.related),
    synonyms: linkWords(parsed.synonyms),
    antonyms: linkWords(parsed.antonyms),
  };
  const relAny = rel.derived.length || rel.related.length || rel.synonyms.length || rel.antonyms.length;

  return {
    $word: parsed.word,
    $lang_code: parsed.lang_code,
    $pos: parsed.pos ?? null,
    $senses: JSON.stringify(senses),
    $sounds: sounds.length ? JSON.stringify(sounds) : null,
    $etymology: parsed.etymology_text ? String(parsed.etymology_text).trim() : null,
    $related: relAny ? JSON.stringify(rel) : null,
  };
}

function parseLine(line) {
  let parsed;
  try {
    parsed = JSON.parse(line);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed.word !== "string" || typeof parsed.lang_code !== "string") return null;
  if (!INCLUDED_LANGS.includes(String(parsed.lang_code).toLowerCase())) return null;
  return compactEntry(parsed);
}

// Import one JSONL file into the given database connection.
// Returns {count, skipped, filtered}.
export async function importJsonl(db, jsonlPath) {
  const insert = db.prepare(INSERT_SQL);
  const insertMany = db.transaction((rows) => {
    for (const row of rows) insert.run(row);
  });

  const rl = createInterface({
    input: createReadStream(jsonlPath, { encoding: "utf-8" }),
    crlfDelay: Infinity,
  });

  let count = 0;
  let skipped = 0;
  let filtered = 0;
  let batch = [];

  for await (const line of rl) {
    if (!line.trim()) continue;
    const row = parseLine(line);
    if (!row) {
      // A row is skipped for either bad JSON or a non-listed language. Both are
      // counted together; the difference only matters for volume reasoning.
      skipped++;
      continue;
    }
    batch.push(row);
    if (batch.length >= BATCH_SIZE) {
      insertMany(batch);
      count += batch.length;
      batch = [];
    }
  }

  if (batch.length > 0) {
    insertMany(batch);
    count += batch.length;
  }

  const total = count + skipped;
  if (total > 0) {
    filtered = skipped;
  }

  return { count, skipped, filtered };
}

// Open (or create) a database at `dbPath` and build the schema + indexes.
export function openDatabase(dbPath, { fresh = false } = {}) {
  const db = new Database(dbPath);
  db.exec("PRAGMA journal_mode = WAL");
  db.exec("PRAGMA synchronous = OFF");
  db.exec("PRAGMA cache_size = -65536"); // 64 MB page cache
  db.exec("PRAGMA temp_store = MEMORY");

  if (fresh) db.exec("DROP TABLE IF EXISTS entries");
  db.exec(TABLE_DDL);
  return db;
}

export function buildIndexes(db) {
  db.exec(INDEX_DDL);
}

// CLI entry point: bun import.js <jsonl-file> <output-db>
// Used directly for validation runs against a small sample file. refresh.js is the
// production path that downloads the real data and produces the staging DB.
if (import.meta.main) {
  const [jsonlPath, outPath] = process.argv.slice(2);
  if (!jsonlPath || !outPath) {
    console.error("usage: bun import.js <input.jsonl> <output.db>");
    process.exit(1);
  }
  const db = openDatabase(outPath, { fresh: true });
  const { count, skipped, filtered } = await importJsonl(db, jsonlPath);
  buildIndexes(db);
  const size = statSync(outPath).size;
  console.log(
    `Imported ${count.toLocaleString()} entries (${skipped.toLocaleString()} skipped, ${filtered.toLocaleString()} lang-filtered) -> ${outPath} (${(size / 1e9).toFixed(2)} GB)`,
  );
  db.close();
}