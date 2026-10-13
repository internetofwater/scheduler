#!/usr/bin/env bash
# Print the first basemap URL template that actually serves tiles.
#
# Usage:
#   BASEMAP_URL=$(bash pick_basemap.sh)          # first working candidate
#   bash pick_basemap.sh --list                  # probe every candidate
#
# A provider that has moved behind an API key does not fail. It answers 200
# with a placeholder image, and that image lands in the thumbnail. Carto did
# exactly that: every request returns the same 2 kB tile reading
# "API KEY REQUIRED". A status check cannot see it, and a file-size floor is
# guesswork because a legitimate tile over empty terrain is small too.
#
# So probe two densely mapped tiles on opposite sides of the world. A real
# basemap returns different bytes for each. A placeholder returns the same
# image both times, because it is not a map. This is the trick the render
# gate already uses: identical hashes mean nothing real was drawn.
#
# Override the whole list with BASEMAP_CANDIDATES, newline separated.
set -u

# London and Tokyo at zoom 13. Both are dense in every basemap worth using.
PROBE_A_Z=13; PROBE_A_X=4093; PROBE_A_Y=2723
PROBE_B_Z=13; PROBE_B_X=7276; PROBE_B_Y=3225
UA="portolan-thumbnails (+https://github.com/portolan-sdi/portolan-skills)"

DEFAULT_CANDIDATES='https://services.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}
https://services.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}
https://services.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}
https://tile.openstreetmap.org/{z}/{x}/{y}.png'

CANDIDATES="${BASEMAP_CANDIDATES:-$DEFAULT_CANDIDATES}"

fill() {  # template z x y
    printf '%s' "$1" | sed -e "s|{z}|$2|g" -e "s|{x}|$3|g" -e "s|{y}|$4|g"
}

digest() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum | cut -d' ' -f1
    else shasum -a 256 | cut -d' ' -f1; fi
}

probe() {  # template -> prints "ok", "placeholder", or "unreachable"
    url_a=$(fill "$1" "$PROBE_A_Z" "$PROBE_A_X" "$PROBE_A_Y")
    url_b=$(fill "$1" "$PROBE_B_Z" "$PROBE_B_X" "$PROBE_B_Y")
    a=$(curl -sfS -m 20 -A "$UA" "$url_a" 2>/dev/null | digest)
    b=$(curl -sfS -m 20 -A "$UA" "$url_b" 2>/dev/null | digest)
    if [ -z "$a" ] || [ -z "$b" ]; then echo unreachable; return; fi
    # An empty body hashes to the digest of nothing, which is still equal.
    if [ "$a" = "$b" ]; then echo placeholder; return; fi
    echo ok
}

# Read into an array first. A `while read` loop fed by a pipe runs in a
# subshell, so an exit inside it leaves the script running and the caller
# sees the wrong status.
CANDIDATE_LIST=()
while IFS= read -r line; do
    [ -n "$line" ] && CANDIDATE_LIST+=("$line")
done <<EOF
$CANDIDATES
EOF

if [ "${1:-}" = "--list" ]; then
    for c in "${CANDIDATE_LIST[@]}"; do
        printf '%-12s %s\n' "$(probe "$c")" "$c"
    done
    exit 0
fi

for c in "${CANDIDATE_LIST[@]}"; do
    if [ "$(probe "$c")" = "ok" ]; then
        printf '%s\n' "$c"
        exit 0
    fi
    printf 'basemap rejected: %s\n' "$c" >&2
done

# Nothing answered. The caller renders on white rather than on a watermark.
exit 1
