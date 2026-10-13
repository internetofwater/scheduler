---
name: portolan-thumbnails
description: Generate framed, checked thumbnails from Portolan collections using chiitiler (MapLibre GL Native). Renders the collection's default style server-side over the PMTiles the collection links to, with an optional basemap, frames every bbox to the browser card's 3:2 shape, accepts an image only after an automated blank probe and a visual review, then refreshes file:size and file:checksum with portolan check and its fix flag. Requires Node.js 24.12 or newer.
---

<!-- drift: depends-on: portolan-cli, portolan-spec -->

# Framed Thumbnails with Chiitiler

Render a collection's default style server-side with
[chiitiler](https://github.com/Kanahiro/chiitiler), frame it to the shape of the
browser card, and check the result before you publish it. The renderer is the
easy half.
Most bad thumbnails come from the bbox.

Use this for any collection whose thumbnail people will look at. The thumbnails
that `portolan add` and `portolan check --fix` draw are the baseline. This replaces
them with an image that matches the portal. The scripts live in `scripts/` beside
this file. Set `SKILL` to this skill's directory once per session.

## Requirements

- Node.js 24.12 or newer, and npm. chiitiler imports `node:sqlite`, so an older
  Node fails at startup with `ERR_UNKNOWN_BUILTIN_MODULE`.
- Git, to clone chiitiler.
- A local checkout of the collection with a `rel: pmtiles` link and a style asset.
- An agent that can view images. Step 5 is not optional.
- DuckDB, optional. Needed only to find a dense subregion in a large collection.

The scripts run on Linux and macOS. They use `nohup` in place of `setsid`, and
fall back from `stat -c%s` and `sha256sum` to `stat -f%z` and `shasum -a 256`.

## Why Framing Is the Whole Job

chiitiler's `/clip` endpoint takes `bbox`, `size`, `quality`, and a style. The
aspect ratio and the zoom both fall out of the bbox, so every framing decision is
a decision about which bbox to send. The raw extent produces tall rectangles in
wide cards, dense layers rendered so far out that tile thinning shows as holes,
and a row of cards that all look alike. Rewriting the bbox reframes the map. It
keeps the aspect ratio of every feature.
`specs/best-practices/conversion-defaults.md`
describes the CLI's own thumbnail as guidance, not conformance.

## Step 1: Read the collection

`read_collection.py` reads the signals from `collection.json` by role and by link
relation, never by file name. Styles are assets with the `style` role, and the
default one also carries `default` (PORTO-CORE-069, PORTO-CORE-070). The PMTiles
is a `rel: pmtiles` link with `pmtiles:layers` (PORTO-FMT-011).

```bash
python3 "$SKILL/scripts/read_collection.py" publico_arbolado/
# {"bbox": "-60.63,-33.92,-60.51,-33.86", "style": ".../styles/default.json",
#  "pmtiles": ".../publico_arbolado.pmtiles", "pmtiles_layers": ["publico_arbolado"],
#  "thumbnail": ".../publico_arbolado.thumb.jpg", "thumbnail_type": "image/jpeg",
#  "geometry_type": "Point", "feature_count": 43525}
```

`bbox` is the starting frame. `thumbnail` and `thumbnail_type` say where to write
and in which format. `pmtiles_layers` is ground truth for `source-layer`.
`feature_count` is the strongest strategy signal, and points and lines need more
zoom than polygons. portolan-cli writes those last two as `geoparquet:*`
properties. The spec defines none of them, and it defines neither
`pmtiles:max_zoom` nor `pmtiles:center`. A catalog from another tool lacks them.
Read the zoom range and
centre from the PMTiles header instead, which always works.

```bash
python3 "$SKILL/scripts/frame.py" \
    --pmtiles publico_arbolado/publico_arbolado.pmtiles
# {"min_zoom": 0, "max_zoom": 13, "center": [-60.578613, -33.888655, 13],
#  "bounds": [-60.638266, -33.923181, -60.512274, -33.860783]}
```

Treat `center` as a hint. tippecanoe sometimes writes a bbox corner rather than a
dense cluster. Confirm it with a feature count before you build a window around it.

## Step 2: Choose a strategy

**A, full extent.** Frame the whole bbox. Right for boundaries, districts, wards,
neighborhoods, watersheds, city limits, and anything with a small feature count.

**B, zoomed window.** A 3:2 window at a chosen zoom, centred on a dense cluster.
Right for parcels, buildings, addresses, service requests, permits, and trees.

These are defaults. Override them when the data says otherwise.

| Signal | Default |
|---|---|
| Polygons, `feature_count` <= 50 | A |
| Name matches boundary, border, limit, district, ward, precinct, zone, tract, county, city, watershed, region, and geometry is polygons | A |
| Any geometry, `feature_count` >= 5000 | B |
| Points or lines, `feature_count` >= 1000 | B |
| Anything else | A when `fill` >= 0.4, otherwise B |
| `aspect` outside 2.2:1 after capping | Decide explicitly and record why |

`fill` and `aspect` come from Step 3. The last two rows mean you run `frame.py`
first and then reconsider.

## Step 3: Compute the bbox

`frame.py` reshapes a bbox to 3:2 in Web Mercator and reports `fill`, `aspect`,
and the effective zoom.

| Knob | Default | Why |
|---|---|---|
| `TARGET_ASPECT` | 1.5 | The browser card is 350 to about 700 px wide and 250 px tall |
| `MARGIN` | 0.05 | Fractional per axis, so the aspect is unchanged |
| `MAX_CONTEXT` | 2.5 | The short axis grows to at most 2.5x the data span, so data stays at 40% of the frame or more |
| `FRAME_ASPECT_LIMIT` | 2.2 | A capped frame still worse than 2.2:1 means full-extent framing failed |
| `--size` | 1024 | Twice the widest realistic card |

### Strategy A

```bash
BBOX=$(python3 "$SKILL/scripts/read_collection.py" publico_barrios/ \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['bbox'])")
python3 "$SKILL/scripts/frame.py" --bbox "$BBOX" --size 1024
# {"bbox": "-60.651220,-33.935211,-60.495382,-33.848971", "aspect": 1.5,
#  "zoom": 13.17, "span_m": 17347.9, "warnings": [], "fill": 0.957}
```

Read `fill` and `aspect` before you render. A `fill` near 1 means the data nearly
fills the frame. A `fill` of exactly 0.4 means `MAX_CONTEXT` capped the growth and
the frame never reached 3:2. When the aspect warning appears you have two moves.
Force the target with `--max-context 99` and accept the extra context, or switch
to Strategy B. Record which you chose.

### Strategy B

```bash
python3 "$SKILL/scripts/frame.py" --center -60.581959,-33.893888 \
    --zoom 15 --size 1024
# {"bbox": "-60.603932,-33.906046,-60.559986,-33.881728", "aspect": 1.5,
#  "zoom": 15.0, ... "strategy": "B"}
```

Zoom governs how much ground the window covers. At `--size 1024` the longest
framed edge runs 78.3 km at zoom 11, 39.1 at 12, 19.6 at 13, 9.8 at 14, 4.9 at 15,
2.4 at 16, and 1.2 at 17.

A useful window is at or above the archive's `max_zoom`, where tile thinning
stops, and at least two levels below the full-extent zoom Strategy A reported,
where it stops looking like an overview. Doubling `--size` to 2048 buys one more
level of detail at the same extent. Downscale afterwards. For a dense point layer
the failure at low zoom is a blob rather than holes. Zoom in until the street
grid shows.

### Finding a Dense Cluster

Try the header centre first. When it is a bbox corner, or when the collection has
several distinct clusters, use DuckDB. GeoParquet 1.1 files carry a `bbox` struct
column, so no spatial extension is needed.

```sql
WITH c AS (
  SELECT (bbox.xmin+bbox.xmax)/2 AS x, (bbox.ymin+bbox.ymax)/2 AS y
  FROM 'publico_arbolado/publico_arbolado.parquet'
), g AS (
  SELECT floor(x/0.0879) gx, floor(y/0.0879) gy, count(*) n FROM c GROUP BY 1,2
), top AS (SELECT gx, gy FROM g ORDER BY n DESC LIMIT 1 OFFSET 0)
SELECT count(*) AS n_in_window, avg(x) AS clon, avg(y) AS clat
FROM c, top
WHERE x BETWEEN (gx-1)*0.0879 AND (gx+2)*0.0879
  AND y BETWEEN (gy-1)*0.0879 AND (gy+2)*0.0879;
```

The cell size is `span_for_zoom(z, SIZE) / 111319.49` degrees: 0.176 at zoom 13,
0.088 at 14, 0.044 at 15. `OFFSET` is the variety lever. Raise it to reach the
second or third densest cluster and keep two collections off the same
neighborhood. Check the count before you commit. Below roughly 100 polygons or
lines, or 200 points, step the zoom down one and try again, up to three times. A
quantile trim on the same centroids, `quantile_cont(x, 0.05)` through
`quantile_cont(x, 0.95)`, handles a scatter with one distant outlier.

## Step 4: render

### Start the Server

`start_server.sh` clones chiitiler into `/tmp/chiitiler` on first use, then starts
it detached with `CHIITILER_PROCESSES=0`, which keeps memory flat across a large
batch. Export `PORT` first when 13579 is taken.

```bash
bash "$SKILL/scripts/start_server.sh"
# health=200
```

### Render one collection

`render_one.sh` calls `read_collection.py`, builds three styles with
`buildstyle.py`, posts them to `/clip`, and runs Gate 1. It writes to the path
the thumbnail asset already points at, in the format the asset's `type` declares.

```bash
WORK=/tmp/portolan-thumbs bash "$SKILL/scripts/render_one.sh" \
    publico_arbolado/ "-60.603932,-33.906046,-60.559986,-33.881728"
# gate1=PASS probe=9312 blank=1516 bytes=88213 out=/.../publico_arbolado.thumb.jpg
```

Pass a format, size, and quality as the third to fifth arguments. `USE_BASEMAP`,
`BASEMAP_URL`, and `BASEMAP_OPACITY` are environment variables. Leave
`BASEMAP_URL` unset and the script picks one that works. See
[Basemap Options](#basemap-options).

`buildstyle.py` rewrites the style in memory and never modifies the published
file. It repoints every source at the local archive through `pmtiles://`. Then
it drops `symbol` layers and puts a white background under an optional basemap.
It also
declares the archive's zoom range on the source, which is the most important
line. Without it MapLibre assumes the source goes to zoom 22. It then asks for a
tile the archive does not contain and draws nothing. That produces an all-basemap
thumbnail.

## Step 5: Check the result

Two gates. Both run before anything is pushed.

### Gate 1, Automated

`render_one.sh` renders a 256-pixel probe over the same bbox with the collection's
layers on a white background and no basemap, plus a blank reference that is the
white background alone. Identical hashes mean the frame received no data. A
probe
within 15% of the blank's file size means almost none did. A render error
returns 500 with a short text body, and `curl -o` writes that text into your
image file. The script deletes the file and prints the body when that happens.

### Gate 2, Visual

View every image. Six questions. Any "no" is a failure.

1. **Data present.** Are features visible, rather than basemap alone? An
   all-basemap, blank, or solid-colour image is a failure, never a valid result.
2. **Data is the subject.** Do features occupy a quarter of the frame or more?
3. **Shape.** Is it landscape and close to 3:2? A portrait image fails unless you
   chose full-extent framing for a boundary layer and said so.
4. **Completeness.** Do continuous fabrics such as parcels run edge to edge
   without holes? Holes mean the render is below the archive's maximum zoom.
5. **Legibility at card size.** Imagine it at 350x230. Pale fills over a light
   basemap read as flat grey.
6. **Distinctness.** Set beside its siblings, is it recognisable?

### Remediation

| Symptom | Fix |
|---|---|
| All basemap, probe empty | Confirm `source-layer` matches `pmtiles_layers`, the rewrite kept the source key, and the bbox intersects the data |
| All basemap, probe has data | The data is under the basemap or fully transparent. Check layer order and paint opacity |
| Black background | The basemap failed to fetch and transparency became black. Check that the shell did not expand the `{z}/{x}/{y}` template |
| Thin or portrait image | Framing was skipped, or `MAX_CONTEXT` capped it. Re-run `frame.py`, then either `--max-context 99` or Strategy B |
| Sliver of data in a big frame | `fill` is too low. Switch to Strategy B, or crop with a quantile trim |
| Scattered holes in a continuous fabric | Shrink the window until the render is at or above `max_zoom`. If it already does, the archive needs retiling |
| Washed out, flat grey | Lower `BASEMAP_OPACITY`, switch to a plainer basemap, or raise the fill opacity in the style |
| Identical to a sibling | Change strategy, raise the `OFFSET` rank, or change the palette |

Retry at most three times per collection, then report what is left and why. Some
collections cannot produce a good thumbnail. A two-point collection is a locator
map and nothing more. Say so rather than burn attempts on it.

Legibility problems that survive reframing belong to the style, not to this skill.
`specs/best-practices/styling.md` covers how to vary default styles across a
catalog.

## Style defects the validator misses

This skill sees each of these as a failed request or a blank image. The
validator reports none of them.

**A `symbol` layer with no `glyphs` endpoint kills the renderer.** MapLibre GL
Native crashes when a layer needs a font it cannot fetch. The worker exits and curl
reports an empty reply rather than a status code. `buildstyle.py` strips `symbol`
layers for this reason. To keep labels in the image, add a `glyphs` entry to the
style and remove that filter.

**A `match` expression needs all-integer or all-string labels.** MapLibre rejects
the style with HTTP 400 when the labels are floats, or when integers sit beside a
string. Wrap the getter as `["to-string", ["get", "col"]]`, write each integral
float as an integer so `1.0` becomes `"1"`, and drop the duplicate labels.

**These paint patterns render blank, and no renderer is at fault.**
`fill-opacity` of `0.0`, a white `circle-color` on the white background, and a
white fill at partial opacity each render nothing. Raise the opacity. Give a white
circle a stroke, and a white fill an outline.

**A sparse collection can be tiled to zoom 0.** An archive that stops at
`maxzoom 0` puts every feature inside one pixel, and no framing recovers that.
Set `pmtiles.max_zoom` in `.portolan/config.yaml` and regenerate.
`--force-pmtiles` implies `--pmtiles`.

```bash
portolan add publico_arbolado/ --force-pmtiles
```

## Step 6: Work through the catalog

Cards are seen side by side, so judge them as a set. Aim for roughly a third
full-extent and the rest zoomed at varying depths. Never put two thumbnails on the
same neighborhood. Raise the `OFFSET` rank instead. Keep a record as you go at
`/tmp/portolan-thumbs/framing.tsv`. It is the evidence that Gate 2 ran.

```
collection	strategy	bbox	zoom	rank	verdict
publico_arbolado	B	-60.603932,-33.906046,-60.559986,-33.881728	15.0	0	pass
publico_barrios	A	-60.651220,-33.935211,-60.495382,-33.848971	13.17	-	pass
```

## Step 7: Refresh checksums and push

The thumbnail asset has `file:size` and `file:checksum`. They MUST match the
bytes the `href` resolves to (PORTO-CORE-030). A re-render changes the bytes, so a
collection you pushed without this step fails the checksum check. Run the fix
from the catalog root, confirm the check is clean, then push.

```bash
portolan check --fix
portolan check
portolan push s3://bucket/catalog
```

Write to the registered `href` by default. `portolan add` leaves a registered
thumbnail alone. `portolan add --force-thumbnails` redraws it, unless the file is
named `thumbnail.<ext>` or `preview.<ext>`, which the CLI treats as an image you
chose. To change the format, delete the old file and set the asset's `href` and
`type` together, then run the same three commands. The thumbnail media type is
`image/png`, `image/jpeg`, or `image/webp` (PORTO-CORE-026). The URL path sets
chiitiler's output format, and `jpg` is an alias for `jpeg`.

## Basemap Options

`render_one.sh` picks a basemap for you. Set `BASEMAP_URL` only to override it.

A basemap that moves behind an API key does not fail. It answers 200 with a
placeholder image, and the renderer uses that image in every thumbnail. A status check
cannot see it, and a file-size floor is guesswork, because a legitimate tile
over empty terrain is small too. So `pick_basemap.sh` requests two densely
mapped tiles on opposite sides of the world and compares the bytes. A real
basemap returns a different image for each. A placeholder returns the same
one twice, because it is not a map.

```bash
bash "$SKILL/scripts/pick_basemap.sh" --list   # probe every candidate
bash "$SKILL/scripts/pick_basemap.sh"          # print the first that works
```

The result is cached in `$WORK/basemap.url`, so a batch probes once. When no
candidate answers, the render falls back to a white background, which reads
better than a watermark.

| Style | URL | Notes |
|-------|-----|-------|
| Esri World Light Gray (default) | `https://services.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}` | Pale street canvas, built to sit under data |
| Esri World Dark Gray | `https://services.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}` | For light-coloured data |
| Esri World Topographic | `https://services.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}` | Terrain and labels; busier |
| Esri World Imagery | `https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}` | Aerial |
| OpenStreetMap | `https://tile.openstreetmap.org/{z}/{x}/{y}.png` | See the usage note below |

The Esri tile path is `{z}/{y}/{x}`. Row comes before column, which is the
reverse of the usual order. Copy the URL rather than assemble it.

**OpenStreetMap is last on purpose.** The
[Tile Usage Policy](https://operations.osmfoundation.org/policies/tiles/)
prohibits systematic or bulk downloading, and a catalog of a few dozen
collections is bulk. It is a usable fallback for one or two images, not a
default for a batch. Send a real `User-Agent` if you use it.

Credit whichever basemap you render. Esri's canvas services carry their own
attribution, and OpenStreetMap requires crediting its contributors. The
catalog README is the right place for that line.

Carto is gone from this list. Every `basemaps.cartocdn.com` style now returns
the same 2 kB tile reading "API KEY REQUIRED" for every request, at every
zoom, worldwide. Set `BASEMAP_CANDIDATES` with your own key if you have one.

`BASEMAP_OPACITY=0.55` keeps the basemap as context. Raise it when sparse data
needs anchoring. Lower it when pale fills get lost. `USE_BASEMAP=false` gives a
plain white background, which renders faster and makes no external requests.

## Troubleshooting

Problems with the image itself are in the remediation table in Step 5. These are
the environment failures.

| Issue | Solution |
|-------|----------|
| `ERR_UNKNOWN_BUILTIN_MODULE: node:sqlite` | Node is too old. Install 24.12 or newer |
| Server will not start | Read `/tmp/chiitiler.log`. When another session holds 13579, export a free `PORT` before both scripts |
| `curl` reports an empty reply | The worker exited. A `symbol` layer with no `glyphs` endpoint is the usual cause |
| Basemap not loading | Check network access, and check the log for a truncated `{z` in the requested URL |
| A watermark across every image | The provider moved behind an API key. Run `pick_basemap.sh --list` and set `BASEMAP_URL` to one that reports `ok` |
| Render returns 500 | The style is invalid, or a source is unreachable. `/tmp/chiitiler.log` names the cause |
| New thumbnail not showing in the browser | The asset `href` still points at the old file. See Step 7 |
