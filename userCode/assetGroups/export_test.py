# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from sqlalchemy import text

from userCode.assetGroups.export import ParquetConfig, move_geoparquet_to_postgis
from userCode.lib.utils import new_sqlalchemy_engine_from_env


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
