# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import hashlib

import geopandas as gpd
from shapely.geometry import Point

from userCode.lib.stac_catalog import (
    GEOCONNEX_PROVIDER_NAME,
    geoconnex_providers,
    humanize_sitemap_id,
    maplibre_style_for_pmtiles,
    multihash_sha256,
    render_thumbnail,
)


def test_multihash_sha256_is_sha2_256_multihash_prefixed():
    data = b"some bytes to checksum"
    digest = multihash_sha256(data)

    # 0x12 (sha2-256) + 0x20 (32-byte length), per the multihash spec
    assert digest.startswith("1220")
    assert digest == f"1220{hashlib.sha256(data).hexdigest()}"


def test_humanize_sitemap_id():
    assert humanize_sitemap_id("monitoring_locations") == "Monitoring Locations"
    assert humanize_sitemap_id("usgs-gages") == "Usgs Gages"


def test_geoconnex_providers_has_producer_and_host_last():
    providers = geoconnex_providers()

    assert len(providers) == 2
    assert providers[0]["roles"] == ["producer"]
    assert providers[-1]["roles"] == ["host"]
    assert all(p["name"] == GEOCONNEX_PROVIDER_NAME for p in providers)
    assert "url" in providers[-1]


def test_maplibre_style_for_pmtiles_references_the_given_layer():
    style = maplibre_style_for_pmtiles("https://example.org/data.pmtiles", "my_layer")

    assert style["version"] == 8
    assert style["sources"]["data"]["url"] == "pmtiles://https://example.org/data.pmtiles"
    assert style["layers"][0]["source-layer"] == "my_layer"


def test_render_thumbnail_writes_a_png(tmp_path):
    gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0), Point(1, 1)]}, crs="EPSG:4326")
    output_path = tmp_path / "thumbnail.png"

    render_thumbnail(gdf, output_path)

    assert output_path.exists()
    assert output_path.stat().st_size > 0
