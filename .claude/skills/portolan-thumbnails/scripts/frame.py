#!/usr/bin/env python3
"""Frame a bbox for thumbnail rendering. Prints JSON."""
import argparse, json, math, struct, sys

R = 6378137.0
EARTH_CIRC = 40075016.686
TARGET_ASPECT, MARGIN, MAX_CONTEXT, FRAME_ASPECT_LIMIT = 1.5, 0.05, 2.5, 2.2

def mx(lon): return math.radians(lon) * R
def inv_mx(x): return math.degrees(x / R)
def inv_my(y): return math.degrees(2 * math.atan(math.exp(y / R)) - math.pi / 2)

def my(lat):
    lat = max(min(lat, 85.05112878), -85.05112878)
    return R * math.log(math.tan(math.pi / 4 + math.radians(lat) / 2))

def eff_zoom(span_m, size_px): return math.log2(EARTH_CIRC * size_px / (256 * span_m))
def span_for_zoom(z, size_px): return EARTH_CIRC * size_px / (256 * 2 ** z)

def clamp(lo, hi, axis, warnings):
    """Keep the frame inside the Mercator world by shifting, never squashing."""
    half = EARTH_CIRC / 2
    if hi - lo > EARTH_CIRC:
        warnings.append("clamped-%s: frame larger than the world" % axis)
        return -half, half
    shift = (half - hi) if hi > half else ((-half - lo) if lo < -half else 0.0)
    if shift:
        warnings.append("shifted-%s: frame hit the world edge" % axis)
    return lo + shift, hi + shift

def frame(bbox, target=TARGET_ASPECT, margin=MARGIN, max_context=MAX_CONTEXT):
    w, s, e, n = bbox
    warnings = []
    x0, x1, y0, y1 = mx(w), mx(e), my(s), my(n)
    # Degenerate axis (a single point, or a line): floor the span at 1 km.
    for axis, (lo, hi) in (("x", (x0, x1)), ("y", (y0, y1))):
        if hi - lo >= 1.0:
            continue
        c = (lo + hi) / 2
        if axis == "x":
            x0, x1 = c - 500, c + 500
        else:
            y0, y1 = c - 500, c + 500
        warnings.append("degenerate-%s: span floored at 1 km" % axis)
    # Margin first: fractional per axis, so the aspect is unchanged.
    dx, dy = (x1 - x0) * margin, (y1 - y0) * margin
    x0, x1, y0, y1 = x0 - dx, x1 + dx, y0 - dy, y1 + dy
    dataW = W = x1 - x0
    dataH = H = y1 - y0
    if W / H < target:      # too tall, widen
        pad = (min(H * target, dataW * max_context) - W) / 2
        if pad > 0:
            x0, x1 = x0 - pad, x1 + pad
    elif W / H > target:    # too wide, heighten
        pad = (min(W / target, dataH * max_context) - H) / 2
        if pad > 0:
            y0, y1 = y0 - pad, y1 + pad
    x0, x1 = clamp(x0, x1, "x", warnings)
    y0, y1 = clamp(y0, y1, "y", warnings)
    fill = min(dataW / (x1 - x0), dataH / (y1 - y0))
    return (inv_mx(x0), inv_my(y0), inv_mx(x1), inv_my(y1)), fill, warnings

def window(clon, clat, z, size_px, target=TARGET_ASPECT):
    """Strategy B: a target-aspect window at zoom z, centred on (clon, clat)."""
    span = span_for_zoom(z, size_px)
    hw, hh = span / 2, span / (2 * target)
    cx, cy = mx(clon), my(clat)
    return inv_mx(cx - hw), inv_my(cy - hh), inv_mx(cx + hw), inv_my(cy + hh)

def report(bbox, size_px, warnings):
    x0, x1, y0, y1 = mx(bbox[0]), mx(bbox[2]), my(bbox[1]), my(bbox[3])
    aspect = (x1 - x0) / (y1 - y0)
    span = max(x1 - x0, y1 - y0)
    if not 1 / FRAME_ASPECT_LIMIT <= aspect <= FRAME_ASPECT_LIMIT:
        warnings.append("aspect %.2f is outside %.1f:1 after capping, "
                        "full-extent framing failed, decide explicitly"
                        % (aspect, FRAME_ASPECT_LIMIT))
    return {"bbox": ",".join("%.6f" % v for v in bbox), "aspect": round(aspect, 3),
            "zoom": round(eff_zoom(span, size_px), 2), "span_m": round(span, 1),
            "warnings": warnings}

def pmtiles_header(path):
    """Zoom range and centre straight from the PMTiles v3 header."""
    h = open(path, "rb").read(127)
    if h[:7] != b"PMTiles":
        raise SystemExit("%s is not a PMTiles archive" % path)
    bounds = [round(v / 1e7, 6) for v in struct.unpack("<iiii", h[102:118])]
    clon, clat = (round(v / 1e7, 6) for v in struct.unpack("<ii", h[119:127]))
    return {"min_zoom": h[100], "max_zoom": h[101],
            "center": [clon, clat, h[118]], "bounds": bounds}

def glue_negatives(argv):
    """Let --bbox -90,38,... work; argparse would read it as an option."""
    out, i = [], 0
    while i < len(argv):
        if argv[i] in ("--bbox", "--center") and i + 1 < len(argv):
            out.append("%s=%s" % (argv[i], argv[i + 1])); i += 2
        else:
            out.append(argv[i]); i += 1
    return out

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bbox", help="w,s,e,n in degrees (full-extent framing)")
    p.add_argument("--center", help="lon,lat for a zoomed window")
    p.add_argument("--zoom", type=float, help="zoom for --center windows")
    p.add_argument("--target", type=float, default=TARGET_ASPECT)
    p.add_argument("--margin", type=float, default=MARGIN)
    p.add_argument("--max-context", type=float, default=MAX_CONTEXT)
    p.add_argument("--size", type=int, default=1024)
    p.add_argument("--pmtiles", help="report zoom range and centre, then exit")
    a = p.parse_args(glue_negatives(sys.argv[1:]))

    if a.pmtiles:
        print(json.dumps(pmtiles_header(a.pmtiles))); return
    if a.center:
        if a.zoom is None:
            p.error("--center needs --zoom")
        clon, clat = (float(v) for v in a.center.split(","))
        out = report(window(clon, clat, a.zoom, a.size, a.target), a.size, [])
        out["fill"], out["strategy"] = None, "B"
    else:
        if not a.bbox:
            p.error("pass --bbox, or --center with --zoom")
        bbox, fill, warnings = frame([float(v) for v in a.bbox.split(",")],
                                     a.target, a.margin, a.max_context)
        out = report(bbox, a.size, warnings)
        out["fill"], out["strategy"] = round(fill, 3), "A"
    print(json.dumps(out))

if __name__ == "__main__":
    main()
