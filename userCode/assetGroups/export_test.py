# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import configparser
from pathlib import Path
import subprocess
from typing import cast

from dagster import AssetCheckResult
import geopandas as gpd
import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

import userCode.assetGroups.export as export
from userCode.assetGroups.export import ParquetConfig, move_geoparquet_to_postgis
from userCode.lib.utils import new_sqlalchemy_engine_from_env


def _qlever_image_from_real_qleverfile() -> str:
    real_qleverfile = Path(__file__).parents[2] / "assets" / "Qleverfile"
    parser = configparser.ConfigParser()
    read_files = parser.read(real_qleverfile)
    assert read_files == [str(real_qleverfile)], (
        f"Expected to read QLever config from {real_qleverfile}"
    )

    image = parser.get("runtime", "IMAGE", fallback=None)
    assert image, f"Expected {real_qleverfile} to define runtime IMAGE"
    return image


def test_qlever_index_and_geoconnex_sparql_query_check(tmp_path, monkeypatch):
    assets_directory = tmp_path / "assets"
    geoconnex_index_directory = assets_directory / "geoconnex_index"
    assets_directory.mkdir()

    test_ttl = assets_directory / "test.ttl"
    test_ttl.write_text(
        """
        @prefix ex: <http://example.com/> .

        ex:subject ex:predicate ex:object .
        """.strip()
    )
    qleverfile = assets_directory / "Qleverfile"
    qlever_image = _qlever_image_from_real_qleverfile()
    qleverfile.write_text(
        f"""
        [data]
        NAME = geoconnex
        DESCRIPTION = test graph
        FORMAT = ttl

        [index]
        INPUT_FILES = ./test.ttl
        CAT_INPUT_FILES = cat ./test.ttl
        SETTINGS_JSON = {{ "num-triples-per-batch": 1000 }}
        PARSER_BUFFER_SIZE = 10MB

        [server]
        PORT = 8888
        ACCESS_TOKEN = ChangeMe

        [runtime]
        SYSTEM = docker
        IMAGE  = {qlever_image}
        """.strip()
    )

    monkeypatch.setattr(export, "ASSETS_DIRECTORY", assets_directory)
    monkeypatch.setattr(export, "GEOCONNEX_INDEX_DIRECTORY", geoconnex_index_directory)

    try:
        export.qlever_index()

        index_files = list(geoconnex_index_directory.glob("geoconnex.*"))
        assert index_files, "Expected qlever_index to build geoconnex index files"

        test_ttl.unlink()
        assert not test_ttl.exists()

        # have to cast this since dagster doesn't type properly
        check_result = cast(
            AssetCheckResult,
            export.geoconnex_sparql_query_check(),
        )
        assert check_result.passed, check_result.description
    finally:
        subprocess.run(
            ["qlever", "--qleverfile", str(qleverfile), "stop"],
            cwd=geoconnex_index_directory
            if geoconnex_index_directory.exists()
            else assets_directory,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )


def test_move_geoparquet_to_postgis(tmp_path):
    engine = new_sqlalchemy_engine_from_env()

    # drop tables if they exist so we ensure we start fresh
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS geoconnex_features"))

    # convert the subset into one parquet file per sitemap with the
    # same columns as the parquet files harvested by nabu
    test_file = Path(__file__).parent / "testdata" / "geoconnex_features_subset.parquet"
    subset = gpd.read_parquet(test_file).rename(columns={"id": "@id"})
    for sitemap_id, features in subset.groupby("geoconnex_sitemap"):
        features.drop(columns=["geoconnex_sitemap", "bbox"]).to_parquet(
            tmp_path / f"{sitemap_id}.parquet"
        )

    move_geoparquet_to_postgis(ParquetConfig(harvested_parquet_directory=str(tmp_path)))

    with engine.connect() as conn:
        # row count check
        result = conn.execute(text("SELECT count(*) FROM geoconnex_features"))
        first_row = result.fetchone()
        assert first_row is not None, "Expected first row to contain info on the count"
        ROWS_IN_SUBSET = 5
        assert first_row[0] == ROWS_IN_SUBSET

        # SRID check
        srid_result = conn.execute(
            text("""
            SELECT ST_SRID(geometry)
            FROM geoconnex_features
            WHERE geometry IS NOT NULL
            LIMIT 1
        """)
        )
        srid = srid_result.scalar()
        assert srid is not None and srid != 0, (
            "Geometry column should have a valid SRID but none was found"
        )
        assert srid == 4326, f"Expected SRID to be 4326 but got {srid}"

        # index on id
        id_index = conn.execute(
            text("""
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = 'geoconnex_features'
              AND indexdef ILIKE '%(id)%'
        """)
        ).fetchone()
        assert id_index is not None, "Expected an index on id column"

        # spatial index on geometry
        geom_index = conn.execute(
            text("""
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = 'geoconnex_features'
              AND indexdef ILIKE '%geom%'
              AND indexdef ILIKE '%gist%'
        """)
        ).fetchone()
        assert geom_index is not None, "Expected a GiST index on geometry column"

        # trigram index for fuzzy searches on feature_name
        feature_name_index = conn.execute(
            text("""
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = 'geoconnex_features'
              AND indexdef ILIKE '%feature_name gin_trgm_ops%'
              AND indexdef ILIKE '%gin%'
        """)
        ).fetchone()
        assert feature_name_index is not None, (
            "Expected a GIN trigram index on feature_name column"
        )

        cols_result = conn.execute(
            text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'geoconnex_features'
            """)
        )
        columns = {row[0] for row in cols_result.fetchall()}

        expected_columns = {
            "geometry",
            "id",
            "geoconnex_sitemap",
            "feature_name",
            "feature_description",
        }

        for column in expected_columns:
            assert column in columns, f"Expected column {column} to be present"


def assert_geometry_column_is_generic_4326(conn):
    """
    The geometry column must accept any geometry type and only EPSG:4326.
    The SRID is checked on the rows and the check constraint rather than
    geometry_columns since PostGIS 3.5 reports 0 there for constraint based SRIDs
    """
    column_type = conn.execute(
        text("""
        SELECT type
        FROM geometry_columns
        WHERE f_table_name = 'geoconnex_features'
    """)
    ).scalar()
    assert column_type == "GEOMETRY"

    srids = {
        row[0]
        for row in conn.execute(
            text("SELECT DISTINCT ST_SRID(geometry) FROM geoconnex_features")
        )
    }
    assert srids == {4326}

    with pytest.raises(IntegrityError):
        with conn.begin_nested():
            conn.execute(
                text(
                    "INSERT INTO geoconnex_features (geometry) "
                    "VALUES (ST_SetSRID(ST_MakePoint(0, 0), 3857))"
                )
            )


def test_move_geoparquet_to_postgis_with_mixed_geometry_types(tmp_path):
    """
    The table's geometry column must accept every geometry type even when
    the first sitemap loaded only contains a single type like points
    """
    from shapely.geometry import Point, Polygon

    engine = new_sqlalchemy_engine_from_env()

    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS geoconnex_features"))

    def write_sitemap(sitemap_id: str, geometries: list):
        gpd.GeoDataFrame(
            {
                "@id": [
                    f"https://geoconnex.us/{sitemap_id}/{i}"
                    for i in range(len(geometries))
                ],
                "feature_name": [f"{sitemap_id} {i}" for i in range(len(geometries))],
                "feature_description": ["test"] * len(geometries),
                "mainstem_uri": [None] * len(geometries),
            },
            geometry=geometries,
            crs="EPSG:4326",
        ).to_parquet(tmp_path / f"{sitemap_id}.parquet")

    # files are loaded in sorted order so the points sitemap creates the table
    write_sitemap("a_points", [Point(0, 0), Point(1, 1)])
    write_sitemap("b_polygons", [Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])])

    move_geoparquet_to_postgis(ParquetConfig(harvested_parquet_directory=str(tmp_path)))

    with engine.connect() as conn:
        geometry_types = {
            row[0]
            for row in conn.execute(
                text("SELECT DISTINCT GeometryType(geometry) FROM geoconnex_features")
            )
        }
        assert geometry_types == {"POINT", "POLYGON"}

        assert_geometry_column_is_generic_4326(conn)


def test_move_geoparquet_to_postgis_skips_sitemaps_without_geometry(tmp_path):
    """
    A sitemap whose features have no geometry (like epa:hmw) must be skipped
    instead of creating the table without a usable geometry column
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    from shapely.geometry import Point

    engine = new_sqlalchemy_engine_from_env()

    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS geoconnex_features"))

    # mirror the parquet nabu harvests for a sitemap without geometry: an entirely
    # null WKB column with geo metadata that has no crs or geometry types;
    # it sorts first so it would otherwise be the one to create the table
    pq.write_table(
        pa.table(
            {
                "@id": ["https://geoconnex.us/no_geometry/0"],
                "feature_name": ["no geometry"],
                "feature_description": ["test"],
                "geometry": pa.array([None], type=pa.binary()),
                "mainstem_uri": pa.array([None], type=pa.string()),
            },
            metadata={
                "geo": '{"columns":{"geometry":{"encoding":"WKB","geometry_types":[]}},'
                '"primary_column":"geometry","version":"1.1.0"}'
            },
        ),
        tmp_path / "c_no_geometry.parquet",
    )
    gpd.GeoDataFrame(
        {
            "@id": ["https://geoconnex.us/b_points/0"],
            "feature_name": ["point"],
            "feature_description": ["test"],
            "mainstem_uri": [None],
        },
        geometry=[Point(0, 0)],
        crs="EPSG:4326",
    ).to_parquet(tmp_path / "b_points.parquet")

    move_geoparquet_to_postgis(ParquetConfig(harvested_parquet_directory=str(tmp_path)))

    with engine.connect() as conn:
        sitemaps = {
            row[0]
            for row in conn.execute(
                text("SELECT DISTINCT geoconnex_sitemap FROM geoconnex_features")
            )
        }
        assert sitemaps == {"b_points"}

        assert_geometry_column_is_generic_4326(conn)


# the geoparquet metadata nabu writes for the structured parquet; it has no crs and
# no geometry types, and declares the bbox column as the covering
NABU_GEO_METADATA = (
    '{"columns":{"geometry":{"encoding":"WKB","geometry_types":[],'
    '"covering":{"bbox":{"xmin":["bbox","xmin"],"ymin":["bbox","ymin"],'
    '"xmax":["bbox","xmax"],"ymax":["bbox","ymax"]}}}},'
    '"primary_column":"geometry","version":"1.1.0"}'
)


def write_nabu_parquet(path: Path, ids: list[str], geometries: list) -> None:
    """Write a parquet file with the same schema and metadata as the structured file harvested by nabu"""
    import pyarrow as pa
    import pyarrow.parquet as pq
    import shapely

    bbox_names = ("xmin", "ymin", "xmax", "ymax")
    bbox_type = pa.struct([(name, pa.float64()) for name in bbox_names])
    pq.write_table(
        pa.table(
            {
                "@id": ids,
                "feature_name": [f"name {id}" for id in ids],
                "feature_description": ["test"] * len(ids),
                "geometry": pa.array(
                    [None if g is None else shapely.to_wkb(g) for g in geometries],
                    type=pa.binary(),
                ),
                "mainstem_uri": pa.array([None] * len(ids), type=pa.string()),
                "bbox": pa.array(
                    [
                        None
                        if g is None
                        else dict(
                            zip(bbox_names, shapely.bounds(g).tolist(), strict=True)
                        )
                        for g in geometries
                    ],
                    type=bbox_type,
                ),
            },
            schema=pa.schema(
                [
                    ("@id", pa.string()),
                    ("feature_name", pa.string()),
                    ("feature_description", pa.string()),
                    ("geometry", pa.binary()),
                    ("mainstem_uri", pa.string()),
                    ("bbox", bbox_type),
                ],
                metadata={"geo": NABU_GEO_METADATA},
            ),
        ),
        path,
        compression="zstd",
    )


def assert_bbox_matches_geometry(rows: dict[str, dict]) -> None:
    """The bbox covering of every row has to be the bounds of its geometry"""
    import shapely

    for row in rows.values():
        if row["geometry"] is None:
            # gpio writes a struct of nulls where nabu wrote a null struct
            assert not row["bbox"] or set(row["bbox"].values()) == {None}
        else:
            bounds = shapely.bounds(shapely.from_wkb(row["geometry"]))
            assert list(row["bbox"].values()) == pytest.approx(bounds.tolist())


def test_optimize_harvested_parquet_file(tmp_path):
    """
    Optimizing must wrap out of range longitudes (like the usgs:gnis points in the
    Aleutians), sort the rows spatially, and keep everything else nabu wrote as is,
    including the bbox covering
    """
    import json

    import pyarrow.parquet as pq
    import shapely
    from shapely.geometry import MultiPoint, Point

    harvested_parquet = tmp_path / "test.parquet"
    # alternate between two far apart areas so the sort has to move rows around
    ids = ["near_0", "far_0", "near_1", "far_1", "no_geometry", "too_far_west"]
    geometries = [
        Point(0, 0),
        Point(100, 60),
        Point(0.1, 0.1),
        MultiPoint([(181, 60), (100.1, 60.1)]),
        None,
        Point(-182.63438, 51.94777, 5),
    ]
    write_nabu_parquet(harvested_parquet, ids, geometries)
    original = pq.read_table(harvested_parquet)

    export.optimize_harvested_parquet_file(harvested_parquet, "test")

    optimized = pq.read_table(harvested_parquet)
    assert optimized.schema.remove_metadata().equals(original.schema.remove_metadata())
    geo = json.loads(pq.read_metadata(harvested_parquet).metadata[b"geo"])
    assert (
        geo["columns"]["geometry"]["covering"]
        == (json.loads(NABU_GEO_METADATA)["columns"]["geometry"]["covering"])
    )
    assert sorted(optimized.column("@id").to_pylist()) == sorted(ids)

    rows = {row["@id"]: row for row in optimized.to_pylist()}
    for id in ids:
        assert rows[id]["feature_name"] == f"name {id}"
    assert rows["no_geometry"]["geometry"] is None

    wrapped = shapely.from_wkb(rows["too_far_west"]["geometry"])
    assert wrapped.x == pytest.approx(177.36562)
    assert wrapped.y == pytest.approx(51.94777)
    assert wrapped.z == 5
    assert shapely.from_wkb(rows["far_1"]["geometry"]).equals(
        MultiPoint([(-179, 60), (100.1, 60.1)])
    )
    # the bbox of the wrapped rows was recomputed and the rest still match
    assert_bbox_matches_geometry(rows)
    assert rows["far_1"]["bbox"]["xmin"] == -179

    # spatially close features end up next to each other
    order = optimized.column("@id").to_pylist()
    assert abs(order.index("near_0") - order.index("near_1")) == 1
    # null geometries can't be placed on the curve so they go at the end
    assert order[-1] == "no_geometry"

    gdf = export.features_from_harvested_parquet(harvested_parquet, "test")
    minx, _, maxx, _ = gdf.total_bounds
    assert -180 <= minx and maxx <= 180


def test_optimize_harvested_parquet_file_keeps_the_bbox_column(tmp_path):
    """Without any wrapped longitudes the bbox column is carried through and sorted with its rows"""
    import json

    import pyarrow.parquet as pq
    from shapely.geometry import Point

    harvested_parquet = tmp_path / "test.parquet"
    ids = ["far", "near", "far_2", "near_2"]
    geometries = [Point(100, 60), Point(0, 0), Point(100.1, 60.1), Point(0.1, 0.1)]
    write_nabu_parquet(harvested_parquet, ids, geometries)

    export.optimize_harvested_parquet_file(harvested_parquet, "test")

    optimized = pq.read_table(harvested_parquet)
    assert optimized.column("@id").to_pylist() != ids
    assert_bbox_matches_geometry({row["@id"]: row for row in optimized.to_pylist()})
    geo = json.loads(pq.read_metadata(harvested_parquet).metadata[b"geo"])
    assert "bbox" in geo["columns"]["geometry"]["covering"]


def test_optimize_harvested_parquet_file_without_geometry(tmp_path):
    """A sitemap without any geometry (like epa:hmw) can still be optimized"""
    import pyarrow.parquet as pq

    harvested_parquet = tmp_path / "test.parquet"
    write_nabu_parquet(harvested_parquet, ["a", "b"], [None, None])
    original = pq.read_table(harvested_parquet)

    export.optimize_harvested_parquet_file(harvested_parquet, "test")

    optimized = pq.read_table(harvested_parquet)
    assert optimized.equals(original)


# DuckDB, which gpio writes with, does not make row groups smaller than its vector
# size so the row group tests need more rows than that
DUCKDB_VECTOR_SIZE = 2048


def row_group_sizes(path: Path) -> list[int]:
    import pyarrow.parquet as pq

    metadata = pq.read_metadata(path)
    return [metadata.row_group(i).num_rows for i in range(metadata.num_row_groups)]


def test_optimize_harvested_parquet_file_row_groups(tmp_path, monkeypatch):
    """Row groups are sized to the target number of compressed bytes"""
    from shapely.geometry import Point

    harvested_parquet = tmp_path / "test.parquet"
    num_rows = DUCKDB_VECTOR_SIZE * 3
    write_nabu_parquet(
        harvested_parquet,
        [str(i) for i in range(num_rows)],
        [Point(i % 360 - 180, i % 180 - 90) for i in range(num_rows)],
    )

    # target a third of the current file per row group
    file_size = harvested_parquet.stat().st_size
    monkeypatch.setattr(
        export, "HARVESTED_PARQUET_ROW_GROUP_TARGET_BYTES", file_size / 3
    )

    export.optimize_harvested_parquet_file(harvested_parquet, "test")

    assert row_group_sizes(harvested_parquet) == [DUCKDB_VECTOR_SIZE] * 3


def test_optimize_harvested_parquet_file_row_group_max_rows(tmp_path, monkeypatch):
    """Row groups never go over the row limit even when they are under the size target"""
    from shapely.geometry import Point

    harvested_parquet = tmp_path / "test.parquet"
    num_rows = DUCKDB_VECTOR_SIZE * 3
    write_nabu_parquet(
        harvested_parquet,
        [str(i) for i in range(num_rows)],
        [Point(i % 360 - 180, i % 180 - 90) for i in range(num_rows)],
    )

    monkeypatch.setattr(
        export, "HARVESTED_PARQUET_ROW_GROUP_MAX_ROWS", DUCKDB_VECTOR_SIZE
    )

    export.optimize_harvested_parquet_file(harvested_parquet, "test")

    assert row_group_sizes(harvested_parquet) == [DUCKDB_VECTOR_SIZE] * 3
