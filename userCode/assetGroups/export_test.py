# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import configparser
from pathlib import Path
import subprocess
from typing import cast

from dagster import AssetCheckResult
from sqlalchemy import text

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


def test_move_geoparquet_to_postgis():
    engine = new_sqlalchemy_engine_from_env()

    # drop tables if they exist so we ensure we start fresh
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS geoconnex_features"))

    test_file = Path(__file__).parent / "testdata" / "geoconnex_features_subset.parquet"
    move_geoparquet_to_postgis(ParquetConfig(geoparquet_path=str(test_file)))

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
