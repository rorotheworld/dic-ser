FROM oven/bun:1-alpine

WORKDIR /app

# Server and import/refresh scripts. No npm install needed: Bun is the runtime.
COPY package.json .
COPY server.js import.js refresh.js ./

# Runtime data (SQLite + downloaded JSONL) lives on a host volume, never in the image.
ENV DATA_DIR=/data
ENV PORT=3000

EXPOSE 3000

# Default: serve. The refresh and import scripts are invoked explicitly by the
# update step with `bun refresh.js` / `bun import.js` as a one-shot run.
CMD ["bun", "server.js"]