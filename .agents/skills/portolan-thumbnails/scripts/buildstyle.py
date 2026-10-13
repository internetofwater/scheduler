#!/usr/bin/env python3
"""Build render, probe and blank styles for one collection.

Usage: buildstyle.py STYLE PMTILES USE_BASEMAP BASEMAP_URL OPACITY OUTDIR
"""
import json, os, sys

style_path, pmtiles_path, use_basemap, basemap_url, basemap_opacity, outdir = sys.argv[1:7]
use_basemap = use_basemap == "true"
basemap_opacity = float(basemap_opacity)

style = json.load(open(style_path))
# Symbol layers need a glyphs endpoint. Without one the renderer worker dies.
# Browsers still draw the labels from the published style.
layers = [l for l in style.get("layers", []) if l.get("type") != "symbol"]

h = open(pmtiles_path, "rb").read(127)
MINZ, MAXZ = h[100], h[101]


# Repoint every source at the local PMTiles file and keep the source keys the
# layers already reference. The declared zoom range makes MapLibre overzoom
# the deepest stored tile instead of asking for one that does not exist.
def pmtiles_source(src=None):
    return {"type": (src or {}).get("type", "vector"),
            "tiles": ["pmtiles://%s/{z}/{x}/{y}" % pmtiles_path],
            "minzoom": MINZ, "maxzoom": MAXZ}


sources = {k: pmtiles_source(v) for k, v in style.get("sources", {}).items()}
sources = sources or {"data": pmtiles_source()}

white = {"id": "background", "type": "background",
         "paint": {"background-color": "#ffffff"}}

render = dict(style)
render["sources"] = dict(sources)
if use_basemap:
    render["sources"]["basemap"] = {
        "type": "raster", "tiles": [basemap_url], "tileSize": 256}
    # White under the basemap: a failed tile fetch then leaves white rather
    # than transparent, and transparent becomes black in JPEG.
    base_layers = [white,
                   {"id": "basemap", "type": "raster", "source": "basemap",
                    "paint": {"raster-opacity": basemap_opacity}}]
else:
    base_layers = [white]
render["layers"] = base_layers + layers

probe = dict(style)
probe["sources"] = sources
probe["layers"] = [white] + layers

blank = {"version": 8, "sources": {}, "layers": [white]}

for name, doc in (("render", render), ("probe", probe), ("blank", blank)):
    with open(os.path.join(outdir, "%s-style.json" % name), "w") as f:
        json.dump(doc, f)
print("ok")
