// dictionary-server: weekly data refresh.
//
// Downloads the current English-Wiktionary wiktextract extract from kaikki.org
// and imports it into a STAGING SQLite (wiktionary.db.new) beside the live
// database. It does NOT swap the file into place itself: after a successful
// import it writes /data/.refresh-ready, which the dictionary server watches and
// hot-swaps (close old DB handle, move new over live, reopen). Lookups keep
// serving old data until the instant of the move.
//
// After a successful import the cached .gz/.jsonl are deleted to reclaim ~25 GB
// of disk. The weekly update step forces a fresh download by deleting the jsonl
// cache before running this; a manual run without --force reuses whatever cache
// still exists, which is what the initial build does against an
// already-downloaded extract.
//
// Run inside the container:
//   bun refresh.js [--force]

import { createGunzip } from "node:zlib";
import { createReadStream, createWriteStream, existsSync } from "node:fs";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";
import { join } from "node:path";
import { mkdir, rename, rm, stat, writeFile } from "node:fs/promises";
import { openDatabase, importJsonl, buildIndexes } from "./import.js";

const DATA_DIR = process.env.DATA_DIR || "./data";
const JSONL_DIR = join(DATA_DIR, "jsonl");
const NEW_DB_PATH = join(DATA_DIR, "wiktionary.db.new");
const READY_MARKER = join(DATA_DIR, ".refresh-ready");

const FORCE = process.argv.includes("--force");

const SOURCE_URL = "https://kaikki.org/dictionary/raw-wiktextract-data.jsonl.gz";
const DEST_GZ = join(JSONL_DIR, "en.jsonl.gz");
const DEST_JSONL = join(JSONL_DIR, "en.jsonl");

// Optional Telegram notification on refresh success/failure. Token and chat come
// from the environment (passed by the update script); if either is absent no
// message is sent, so the public repo carries no secrets.
async function notify(message) {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chat = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chat) return;
  try {
    await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ chat_id: chat, text: message }),
    });
  } catch (err) {
    console.error("telegram notify failed:", err);
  }
}

async function main() {
  for (const dir of [DATA_DIR, JSONL_DIR]) {
    await mkdir(dir, { recursive: true });
  }

  if (FORCE) {
    await rm(DEST_GZ, { force: true });
    await rm(DEST_JSONL, { force: true });
    console.log("--force: discarded cached download; will refetch.");
  }

  // 1. Download unless already present.
  if (!existsSync(DEST_GZ)) {
    console.log(`Downloading ${SOURCE_URL}`);
    const res = await fetch(SOURCE_URL);
    if (!res.ok || !res.body) throw new Error(`download failed: HTTP ${res.status}`);
    const tmpGz = `${DEST_GZ}.tmp`;
    await pipeline(Readable.fromWeb(res.body), createWriteStream(tmpGz));
    await rename(tmpGz, DEST_GZ);
    const { size } = await stat(DEST_GZ);
    console.log(`Downloaded ${(size / 1e9).toFixed(2)} GB compressed`);
  } else {
    console.log("Using existing download (pass --force to refetch)");
  }

  // 2. Decompress to JSONL (tmp, then rename).
  if (!existsSync(DEST_JSONL)) {
    console.log("Decompressing...");
    const tmpJsonl = `${DEST_JSONL}.tmp`;
    await pipeline(createReadStream(DEST_GZ), createGunzip(), createWriteStream(tmpJsonl));
    await rename(tmpJsonl, DEST_JSONL);
  } else {
    console.log("Using existing JSONL");
  }

  // 3. Import into a FRESH staging database. The live wiktionary.db is untouched.
  await rm(NEW_DB_PATH, { force: true });
  console.log("Importing into staging database...");
  const db = openDatabase(NEW_DB_PATH, { fresh: true });
  const { count, skipped } = await importJsonl(db, DEST_JSONL);
  buildIndexes(db);
  // Checkpoint the WAL into the main file so the rename in the hot-swap moves a
  // self-contained DB (no dangling -wal/-shm next to it after reopen).
  db.exec("PRAGMA wal_checkpoint(TRUNCATE)");
  db.close();

  // Drop any leftover WAL/shm sidecars from the staging file.
  await Promise.all([
    rm(`${NEW_DB_PATH}-wal`, { force: true }),
    rm(`${NEW_DB_PATH}-shm`, { force: true }),
  ]).catch(() => {});

  const { size } = await stat(NEW_DB_PATH);
  console.log(
    `Staging DB ready: ${count.toLocaleString()} entries, ${(size / 1e9).toFixed(2)} GB, ${skipped.toLocaleString()} skipped`,
  );

  // Signal the server to hot-swap. Written only after the DB is fully built.
  await writeFile(READY_MARKER, new Date().toISOString());
  console.log(`Wrote ${READY_MARKER}; dictionary server will hot-swap within its poll window.`);

  await notify(
    `dictionary-server refresh OK: ${count.toLocaleString()} English entries, ${(size / 1e9).toFixed(2)} GB. Hot-swapping.`,
  );

  // 4. Reclaim disk: the cached extract is only needed to rebuild. The weekly
  //    update forces a fresh fetch anyway.
  await rm(DEST_JSONL, { force: true });
  await rm(DEST_GZ, { force: true });
  console.log("Deleted cached JSONL extract (~25 GB).");
}

main().catch(async (err) => {
  console.error("refresh failed:", err);
  await notify(`dictionary-server refresh FAILED: ${String(err?.message || err)}. Old data still serving.`).catch(() => {});
  // Remove a half-built staging DB so a retry starts clean, and leave the cached
  // extract alone (a retry can reuse it instead of re-downloading).
  await rm(NEW_DB_PATH, { force: true }).catch(() => {});
  await rm(`${NEW_DB_PATH}-wal`, { force: true }).catch(() => {});
  await rm(`${NEW_DB_PATH}-shm`, { force: true }).catch(() => {});
  process.exit(1);
});