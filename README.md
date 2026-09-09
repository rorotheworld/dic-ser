# dic-ser

Self-hosted English dictionary API from Wiktionary data. Serves clean JSON
definitions from a local SQLite database, so lookups are instant and work
offline.

Useful as a drop-in backend for a dictionary UI or a metasearch plugin that
would otherwise talk to a flaky public API.

## Data

The data comes from [kaikki.org](https://kaikki.org/dictionary/rawdata.html),
which republishes the English Wiktionary as machine-readable JSONL at least once
a week - the same source `api.dictionaryapi.dev` and wiktapi are built on.

`import.js` trims the raw extract in two ways:

- **Language filter.** The extract carries every language glossed in English
  (~10.8M entries); by default only actual English words (`lang_code: "en"`,
  ~1.5M) are imported. Set `INCLUDE_LANGS` to import others.
- **Field projection.** Only the fields a dictionary card needs are stored:
  glosses, example sentences, synonym/antonym names, IPA, audio URLs,
  etymology, and related words. Raw entries can be tens of KB; the compact
  projection keeps the database around a few hundred MB.

Data is CC BY-SA 3.0 / GFDL (Wiktionary).

## Layout

```
server.js     HTTP API (Bun.serve on :3000)
import.js     JSONL -> SQLite import
refresh.js    download kaikki extract -> import to a staging DB
Dockerfile    container image
```

## Running

Build and run (data lives on a host volume):

```
docker build -t dic-ser .
docker run -p 3000:3000 -v ./data:/data dic-ser
```

First run needs the database built:

```
docker run --rm -v ./data:/data dic-ser bun refresh.js
```

`refresh.js` supports `--force` to discard a cached download and fetch the
latest extract. After a successful import it writes a `.refresh-ready` marker;
when that marker exists the server hot-swaps: it closes the current read handle,
renames `wiktionary.db.new` over `wiktionary.db`, and reopens. Live lookups keep
serving from the old data until the instant of the swap, so a refresh never
interrupts the service. The cached download is deleted after a successful import
to reclaim disk.

## API

- `GET /health` -> `{status: "ok", words: <count>}`
- `GET /api/en/{word}` -> full entry (glosses with examples and synonyms,
  sounds, etymology, related words). 404 when the word is absent
- `GET /api/en/{word}?lang=<code>` -> word language override (defaults to `en`)

## Licence

MIT for this code. The data is Wiktionary CC BY-SA 3.0 / GFDL; see kaikki.org
for their repository and attribution requirements.
