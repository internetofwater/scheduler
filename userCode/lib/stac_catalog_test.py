# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import hashlib

import geopandas as gpd
import pystac
from shapely.geometry import Point

from userCode.lib.stac_catalog import (
    GEOCONNEX_PROVIDER_NAME,
    geoconnex_providers,
    humanize_sitemap_id,
    maplibre_style_for_pmtiles,
    multihash_sha256,
    patch_sitemap_collection,
    render_thumbnail,
    sitemap_dataset_descriptions,
    stac_collection_id,
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


def test_stac_collection_id_is_portolan_valid():
    assert stac_collection_id("monitoring_locations") == "monitoring_locations"
    assert stac_collection_id("ca_gage_assessment:gages") == "ca_gage_assessment-gages"
    assert stac_collection_id("USGS/Gages::v2") == "usgs-gages-v2"
    assert stac_collection_id("_leading") == "leading"


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
    assert (
        style["sources"]["data"]["url"] == "pmtiles://https://example.org/data.pmtiles"
    )
    assert all(layer["source-layer"] == "my_layer" for layer in style["layers"])


def test_maplibre_style_for_pmtiles_renders_every_geometry_type():
    style = maplibre_style_for_pmtiles("https://example.org/data.pmtiles", "my_layer")

    layer_types = {layer["type"] for layer in style["layers"]}
    assert layer_types == {"fill", "line", "circle"}


def test_sitemap_dataset_descriptions_maps_ids_to_descriptions():
    index_xml = """<?xml version='1.0' encoding='utf-8'?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:geoconnex="https://geoconnex.us">
<sitemap>
  <loc>https://geoconnex.us/sitemap/usbr/rise.xml</loc>
  <geoconnex:sitemap_id>usbr:rise</geoconnex:sitemap_id>
  <geoconnex:dataset_description> Locations in the USBR RISE API </geoconnex:dataset_description>
</sitemap><sitemap>
  <loc>https://geoconnex.us/sitemap/no/description.xml</loc>
  <geoconnex:sitemap_id>no:description</geoconnex:sitemap_id>
</sitemap>
</sitemapindex>"""

    assert sitemap_dataset_descriptions(index_xml) == {
        "usbr:rise": "Locations in the USBR RISE API"
    }


def test_patch_sitemap_collection_adds_pmtiles_as_a_visual_asset(tmp_path):
    collection = pystac.Collection(
        id="usbr_rise",
        description="test",
        extent=pystac.Extent(
            pystac.SpatialExtent([[-180, -90, 180, 90]]),
            pystac.TemporalExtent([[None, None]]),
        ),
    )
    collection.add_asset(
        "data", pystac.Asset(href="https://example.org/data.parquet", roles=["data"])
    )
    collection_path = tmp_path / "collection.json"
    collection.set_self_href(str(collection_path))
    collection.save_object(include_self_link=False)

    pmtiles_url = "https://example.org/usbr:rise.pmtiles"
    patch_sitemap_collection(
        collection_path,
        sitemap_id="usbr:rise",
        parquet_size=1,
        parquet_checksum="1220abc",
        pmtiles_url=pmtiles_url,
        pmtiles_layer="usbr_rise",
    )

    patched = pystac.Collection.from_file(str(collection_path))
    pmtiles_asset = patched.assets["pmtiles"]
    assert pmtiles_asset.href == pmtiles_url
    assert pmtiles_asset.media_type == "application/vnd.pmtiles"
    assert pmtiles_asset.roles == ["visual"]
    # the default style's source must point at the same file so viewers
    # apply the style to this asset
    style = maplibre_style_for_pmtiles(pmtiles_url, "usbr_rise")
    assert style["sources"]["data"]["url"] == f"pmtiles://{pmtiles_asset.href}"


def test_render_thumbnail_writes_a_png(tmp_path):
    gdf = gpd.GeoDataFrame({"geometry": [Point(0, 0), Point(1, 1)]}, crs="EPSG:4326")
    output_path = tmp_path / "thumbnail.png"

    render_thumbnail(gdf, output_path)

    assert output_path.exists()
    assert output_path.stat().st_size > 0
