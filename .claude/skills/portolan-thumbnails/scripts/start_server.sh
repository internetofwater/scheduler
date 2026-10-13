#!/usr/bin/env bash
# Clone chiitiler once, then start it detached from this shell.
# Usage: start_server.sh   (honours PORT, default 13579)
set -euo pipefail

CHIITILER_DIR="${CHIITILER_DIR:-/tmp/chiitiler}"
PORT="${PORT:-13579}"
LOG="${LOG:-/tmp/chiitiler.log}"

if [ ! -d "$CHIITILER_DIR/node_modules" ]; then
    rm -rf "$CHIITILER_DIR"
    git clone --depth 1 \
        https://github.com/Kanahiro/chiitiler "$CHIITILER_DIR"
    (cd "$CHIITILER_DIR" && npm install --silent)
fi

if curl -s -o /dev/null "http://localhost:$PORT/health"; then
    echo "chiitiler already listening on $PORT"
    exit 0
fi

# nohup and a background job outlive the shell on Linux and macOS.
# setsid does not exist on macOS.
cd "$CHIITILER_DIR"
CHIITILER_PROCESSES=0 nohup npx tsx src/main.ts tile-server \
    --port "$PORT" --cache memory \
    > "$LOG" 2>&1 < /dev/null &
disown

for _ in $(seq 1 40); do
    curl -s -o /dev/null "http://localhost:$PORT/health" && break
    sleep 1
done
curl -s -o /dev/null -w "health=%{http_code}\n" "http://localhost:$PORT/health"
