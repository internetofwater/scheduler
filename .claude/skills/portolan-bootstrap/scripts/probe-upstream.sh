#!/usr/bin/env bash
# Probe one upstream asset for the capabilities a metadata-only
# mirror depends on: ranged GET, HEAD size, and CORS preflight.
# A failed probe limits clients. It does not make the catalog
# non-conformant (PORTO-CORE-073). Usage:
#   probe-upstream.sh <ASSET_URL> [ORIGIN]
set -euo pipefail

ASSET_URL="${1:?usage: probe-upstream.sh <ASSET_URL> [ORIGIN]}"
ORIGIN="${2:-https://example.org}"
OUT="$(mktemp -d)"

echo "== ranged GET (first 16 KiB)"
curl -sS -D "$OUT/range-headers.txt" -o "$OUT/range.bin" \
  -H 'Range: bytes=0-16383' \
  -H "Origin: $ORIGIN" \
  "$ASSET_URL"
grep -iE '^(HTTP|accept-ranges|content-range|content-length|content-encoding)' \
  "$OUT/range-headers.txt"
echo "bytes transferred: $(wc -c < "$OUT/range.bin")"

echo "== HEAD (compare Content-Length with the Content-Range total)"
curl -sS -I "$ASSET_URL" \
  | grep -iE '^(HTTP|content-length|accept-ranges)'

echo "== CORS preflight"
curl -sS -D "$OUT/cors-headers.txt" -o /dev/null -X OPTIONS \
  -H "Origin: $ORIGIN" \
  -H 'Access-Control-Request-Method: GET' \
  -H 'Access-Control-Request-Headers: Range,If-Match,If-Modified-Since,If-None-Match,If-Unmodified-Since' \
  "$ASSET_URL"
grep -iE '^(HTTP|access-control-)' "$OUT/cors-headers.txt"

echo "headers saved under $OUT"
