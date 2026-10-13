#!/usr/bin/env python3
"""Read the signals a thumbnail render needs from one collection.json.

Prints JSON with absolute paths. Resolves the style through the asset roles
(`style`, plus `default` when there is more than one) and the PMTiles through
the `rel: pmtiles` link, never through file names.
"""
import json, os, sys

coll_dir = os.path.abspath(sys.argv[1])
c = json.load(open(os.path.join(coll_dir, "collection.json")))
assets = c.get("assets", {})


def local(href):
    if href.startswith(("http://", "https://", "s3://", "gs://")):
        raise SystemExit("%s is remote; render from a local checkout" % href)
    return os.path.normpath(os.path.join(coll_dir, href))


styles = {k: a for k, a in assets.items() if "style" in (a.get("roles") or [])}
default = [k for k, a in styles.items() if "default" in a["roles"]]
if default:
    style_key = default[0]
elif len(styles) == 1:
    style_key = next(iter(styles))
else:
    raise SystemExit("no style asset carries the default role (PORTO-CORE-070)")

pm = next((l for l in c.get("links", []) if l.get("rel") == "pmtiles"), None)
if pm is None:
    raise SystemExit("no rel: pmtiles link (PORTO-FMT-011)")

thumb_key = next((k for k, a in assets.items()
                  if "thumbnail" in (a.get("roles") or [])), None)
if thumb_key is None:
    raise SystemExit("no thumbnail-role asset; run portolan add first")

out = {
    "bbox": ",".join(str(v) for v in c["extent"]["spatial"]["bbox"][0]),
    "style": local(styles[style_key]["href"]),
    "style_key": style_key,
    "pmtiles": local(pm["href"]),
    "pmtiles_layers": pm.get("pmtiles:layers", []),
    "thumbnail": local(assets[thumb_key]["href"]),
    "thumbnail_type": assets[thumb_key].get("type"),
    # portolan-cli private properties. Absent on catalogs other tools wrote.
    "geometry_type": c.get("geoparquet:geometry_type"),
    "feature_count": c.get("geoparquet:feature_count"),
}
print(json.dumps(out))
