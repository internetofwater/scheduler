# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import hashlib
import json

import geopandas as gpd
import pystac
import pytest
from shapely.geometry import Point, box

from userCode.lib.stac_catalog import (
    CATALOG_LOGO_FILE,
    CATALOG_LOGO_HREF,
    GEOCONNEX_PROVIDER_NAME,
    MAINTAINER_PROVIDER_URL,
    SitemapDatasetMetadata,
    THUMBNAIL_HEIGHT_PX,
    THUMBNAIL_WIDTH_PX,
    add_catalog_icon,
    geoconnex_providers,
    humanize_sitemap_id,
    maplibre_style_for_pmtiles,
    multihash_sha256,
    multihash_sha256_file,
    patch_sitemap_collection,
    register_sitemap_collection,
    render_thumbnail,
    sitemap_collection_title,
    sitemap_dataset_metadata,
    stac_collection_id,
    thumbnail_extent,
    thumbnail_features,
    thumbnail_frame,
)


def test_multihash_sha256_is_sha2_256_multihash_prefixed():
    data = b"some bytes to checksum"
    digest = multihash_sha256(data)

    # 0x12 (sha2-256) + 0x20 (32-byte length), per the multihash spec
    assert digest.startswith("1220")
    assert digest == f"1220{hashlib.sha256(data).hexdigest()}"


def test_multihash_sha256_file_matches_in_memory_digest(tmp_path):
    data = b"some bytes to checksum" * 1000
    path = tmp_path / "data.bin"
    path.write_bytes(data)

    # a small chunk size so the file is read in many chunks
    assert multihash_sha256_file(path, chunk_size=7) == multihash_sha256(data)


def test_humanize_sitemap_id():
    assert humanize_sitemap_id("monitoring_locations") == "Monitoring Locations"
    assert humanize_sitemap_id("usgs-gages") == "Usgs Gages"


def test_sitemap_collection_title_joins_dataset_name_and_sitemap_id():
    assert (
        sitemap_collection_title("usbr:rise", "USBR RISE Locations")
        == "USBR RISE Locations (usbr:rise)"
    )
    assert sitemap_collection_title("usgs-gages", None) == "Usgs Gages"


def test_stac_collection_id_is_portolan_valid():
    assert stac_collection_id("monitoring_locations") == "monitoring_locations"
    assert stac_collection_id("ca_gage_assessment:gages") == "ca_gage_assessment-gages"
    assert stac_collection_id("USGS/Gages::v2") == "usgs-gages-v2"
    assert stac_collection_id("_leading") == "leading"


def test_geoconnex_providers_has_producer_processor_and_host_last():
    providers = geoconnex_providers()

    assert len(providers) == 3
    assert providers[0]["roles"] == ["producer"]
    assert providers[1]["roles"] == ["processor"]
    assert providers[1]["url"] == MAINTAINER_PROVIDER_URL
    assert providers[-1]["roles"] == ["host"]
    assert providers[0]["name"] == providers[-1]["name"] == GEOCONNEX_PROVIDER_NAME
    # STAC validators reject anything but a web IRI here, e.g. an email or mailto:
    assert all(p["url"].startswith("https://") for p in providers if "url" in p)


def test_add_catalog_icon_links_and_copies_the_logo_once(tmp_path):
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(
        json.dumps({"type": "Catalog", "links": [{"rel": "root", "href": "./"}]})
    )

    # running twice must not duplicate the link
    add_catalog_icon(tmp_path)
    add_catalog_icon(tmp_path)

    links = json.loads(catalog_path.read_text())["links"]
    icons = [link for link in links if link["rel"] == "icon"]
    assert len(icons) == 1
    assert icons[0]["href"] == CATALOG_LOGO_HREF
    assert icons[0]["type"] == "image/png"
    assert any(link["rel"] == "root" for link in links)
    assert (tmp_path / CATALOG_LOGO_HREF).read_bytes() == CATALOG_LOGO_FILE.read_bytes()


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


def test_sitemap_dataset_metadata_maps_ids_to_names_descriptions_and_links():
    index_xml = """<?xml version='1.0' encoding='utf-8'?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:geoconnex="https://geoconnex.us">
<sitemap>
  <loc>https://geoconnex.us/sitemap/usbr/rise.xml</loc>
  <geoconnex:sitemap_id>usbr:rise</geoconnex:sitemap_id>
  <geoconnex:dataset_name> USBR RISE Locations </geoconnex:dataset_name>
  <geoconnex:dataset_description> Locations in the USBR RISE API </geoconnex:dataset_description>
  <geoconnex:dataset_documentation_link> https://data.usbr.gov/rise-api </geoconnex:dataset_documentation_link>
  <geoconnex:source_code_link>
    https://github.com/internetofwater/geoconnex_bulk_runners/tree/main/usbr%3Arise
  </geoconnex:source_code_link>
  <geoconnex:add_associated_mainstems>True</geoconnex:add_associated_mainstems>
</sitemap><sitemap>
  <loc>https://geoconnex.us/sitemap/no/mainstems.xml</loc>
  <geoconnex:sitemap_id>no:mainstems</geoconnex:sitemap_id>
  <geoconnex:add_associated_mainstems>False</geoconnex:add_associated_mainstems>
</sitemap><sitemap>
  <loc>https://geoconnex.us/sitemap/no/description.xml</loc>
  <geoconnex:sitemap_id>no:description</geoconnex:sitemap_id>
</sitemap>
</sitemapindex>"""

    assert sitemap_dataset_metadata(index_xml) == {
        "usbr:rise": SitemapDatasetMetadata(
            name="USBR RISE Locations",
            description="Locations in the USBR RISE API",
            documentation_link="https://data.usbr.gov/rise-api",
            source_code_link="https://github.com/internetofwater/geoconnex_bulk_runners/tree/main/usbr%3Arise",
            add_associated_mainstems=True,
        ),
        "no:mainstems": SitemapDatasetMetadata(add_associated_mainstems=False),
        "no:description": SitemapDatasetMetadata(),
    }


def test_register_sitemap_collection_puts_dataset_description_first(
    tmp_path, monkeypatch
):
    portolan_calls = []

    def fake_run_portolan(args, cwd):
        portolan_calls.append(args)
        return {"data": {"collection_path": str(tmp_path / "collection.json")}}

    monkeypatch.setattr("userCode.lib.stac_catalog._run_portolan", fake_run_portolan)

    register_sitemap_collection(
        tmp_path,
        sitemap_id="usbr:rise",
        parquet_url="https://example.com/usbr:rise.parquet",
        bbox=[-120.0, 30.0, -100.0, 45.0],
        dataset_name="USBR RISE Locations",
        dataset_description="Locations in the USBR RISE API",
        dataset_documentation_link="https://data.usbr.gov/rise-api",
        source_code_link="https://github.com/internetofwater/geoconnex_bulk_runners",
        add_associated_mainstems=True,
    )

    args = portolan_calls[0]
    assert args[args.index("--title") + 1] == "USBR RISE Locations (usbr:rise)"
    description = args[args.index("--description") + 1]
    (
        dataset_part,
        documentation_part,
        harvest_part,
        source_code_part,
        mainstem_part,
    ) = description.split("\n\n")
    assert dataset_part == "Locations in the USBR RISE API"
    assert (
        documentation_part == "More info can be found at https://data.usbr.gov/rise-api"
    )
    assert harvest_part == (
        "All features in this dataset derive from the 'usbr:rise' sitemap in Geoconnex."
    )
    assert source_code_part == (
        "The source code that generates this sitemap can be found at "
        "https://github.com/internetofwater/geoconnex_bulk_runners"
    )
    assert mainstem_part == (
        "Mainstem association was enabled for this dataset so features may "
        "have an associated Geoconnex mainstem."
    )


def test_register_sitemap_collection_notes_disabled_mainstem_association(
    tmp_path, monkeypatch
):
    portolan_calls = []

    def fake_run_portolan(args, cwd):
        portolan_calls.append(args)
        return {"data": {"collection_path": str(tmp_path / "collection.json")}}

    monkeypatch.setattr("userCode.lib.stac_catalog._run_portolan", fake_run_portolan)

    register_sitemap_collection(
        tmp_path,
        sitemap_id="ref:hu12",
        parquet_url="https://example.com/ref:hu12.parquet",
        bbox=[-120.0, 30.0, -100.0, 45.0],
        add_associated_mainstems=False,
    )

    args = portolan_calls[0]
    description = args[args.index("--description") + 1]
    assert description.endswith(
        "\n\nMainstem association was disabled for this dataset so features will "
        "not have an associated Geoconnex mainstem."
    )


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

    # no basemap so the test does not fetch tiles over the network
    render_thumbnail(gdf, output_path, basemap=False)

    assert output_path.exists()
    assert output_path.stat().st_size > 0


def test_thumbnail_features_samples_large_collections():
    gdf = gpd.GeoDataFrame(
        {"geometry": [Point(i, i) for i in range(100)]}, crs="EPSG:4326"
    )

    sample = thumbnail_features(gdf, max_features=10)

    assert len(sample) == 10
    # the sample is seeded so thumbnails are stable across runs
    assert sample.index.equals(thumbnail_features(gdf, max_features=10).index)
    assert len(thumbnail_features(gdf, max_features=1000)) == 100


def test_thumbnail_extent_leaves_out_small_outliers():
    # a large area of data, plus a tiny island on the other side of the world
    gdf = gpd.GeoDataFrame(
        {"geometry": [box(-120, 30, -80, 45), box(145, 13, 145.1, 13.1)]},
        crs="EPSG:4326",
    )

    assert thumbnail_extent(gdf) == (-120, 30, -80, 45)


def test_thumbnail_frame_is_the_thumbnail_aspect_ratio():
    for bounds in [(-120, 30, -80, 45), (-100, 25, -99, 49), (-90, 30, -90, 30)]:
        min_x, min_y, max_x, max_y = thumbnail_frame(bounds)

        assert (max_x - min_x) / (max_y - min_y) == pytest.approx(
            THUMBNAIL_WIDTH_PX / THUMBNAIL_HEIGHT_PX
        )


def test_register_sitemap_collection_skips_missing_dataset_metadata(
    tmp_path, monkeypatch
):
    portolan_calls = []

    def fake_run_portolan(args, cwd):
        portolan_calls.append(args)
        return {"data": {"collection_path": str(tmp_path / "collection.json")}}

    monkeypatch.setattr("userCode.lib.stac_catalog._run_portolan", fake_run_portolan)

    register_sitemap_collection(
        tmp_path,
        sitemap_id="usgs-gages",
        parquet_url="https://example.com/usgs-gages.parquet",
        bbox=[-120.0, 30.0, -100.0, 45.0],
    )

    args = portolan_calls[0]
    assert args[args.index("--title") + 1] == "Usgs Gages"
    description = args[args.index("--description") + 1]
    assert "\n\n" not in description
    assert "More info" not in description
    assert "Mainstem association" not in description
    assert "source code" not in description
