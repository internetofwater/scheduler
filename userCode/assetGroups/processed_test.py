# Copyright 2026 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box

from userCode.assetGroups.processed import (
    MAINSTEM_URI_COLUMN,
    REFERENCE_CATCHMENTS_TABLE_COLUMNS,
    join_catchments_to_mainstems,
)


def catchments() -> gpd.GeoDataFrame:
    # the reference catchments store their comid as a float
    return gpd.GeoDataFrame(
        {
            "fid": [1, 2, 3],
            "featureid": [101.0, 102.0, 103.0],
            "areasqkm": [1.5, 2.5, 3.5],
            "vpuid": ["14", "14", "15"],
            "rpuid": ["14a", "14a", "15a"],
            "geom": [box(0, 0, 1, 1), box(1, 0, 2, 1), box(2, 0, 3, 1)],
        },
        geometry="geom",
        crs="EPSG:4326",
    )


def lookup() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "uri": [
                "https://geoconnex.us/ref/mainstems/1",
                "https://geoconnex.us/ref/mainstems/2",
            ],
            "comid": [101, 102],
        }
    )


def mainstems() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "uri": [
                "https://geoconnex.us/ref/mainstems/1",
                "https://geoconnex.us/ref/mainstems/2",
                "https://geoconnex.us/ref/mainstems/3",
            ],
            "superseded": [False, False, True],
        }
    )


def test_join_catchments_to_mainstems_keeps_every_catchment():
    joined = join_catchments_to_mainstems(catchments(), lookup(), mainstems())

    assert joined["Catchment_featureid"].tolist() == [101, 102, 103]
    assert joined[MAINSTEM_URI_COLUMN].tolist()[:2] == [
        "https://geoconnex.us/ref/mainstems/1",
        "https://geoconnex.us/ref/mainstems/2",
    ]
    # catchments whose flowline is not on a mainstem are kept without one
    assert pd.isna(joined[MAINSTEM_URI_COLUMN].iloc[2])
    assert joined.crs == "EPSG:4326"
    # the columns described in the STAC catalog are exactly the ones written
    assert joined.columns.tolist() == [
        column["name"] for column in REFERENCE_CATCHMENTS_TABLE_COLUMNS
    ]


def test_join_catchments_to_mainstems_reprojects_to_wgs84():
    projected = catchments().to_crs("EPSG:5070")

    joined = join_catchments_to_mainstems(projected, lookup(), mainstems())

    assert joined.crs == "EPSG:4326"
    assert joined.total_bounds == pytest.approx([0, 0, 3, 1])


def test_join_catchments_to_mainstems_accepts_a_geometry_column_named_geometry():
    # reading the reference catchments gpkg with pyogrio names it 'geometry'
    named_geometry = catchments().rename_geometry("geometry")

    joined = join_catchments_to_mainstems(named_geometry, lookup(), mainstems())

    assert joined.geometry.name == "geometry"
    assert len(joined) == 3


def test_join_catchments_to_mainstems_rejects_superseded_mainstems():
    superseded_lookup = pd.concat(
        [
            lookup(),
            pd.DataFrame(
                {"uri": ["https://geoconnex.us/ref/mainstems/3"], "comid": [103]}
            ),
        ]
    )

    with pytest.raises(ValueError, match="superseded"):
        join_catchments_to_mainstems(catchments(), superseded_lookup, mainstems())


def test_join_catchments_to_mainstems_rejects_unknown_mainstems():
    unknown_lookup = lookup()
    unknown_lookup.loc[0, "uri"] = "https://geoconnex.us/ref/mainstems/999"

    with pytest.raises(ValueError, match="not in the mainstems release"):
        join_catchments_to_mainstems(catchments(), unknown_lookup, mainstems())


def test_join_catchments_to_mainstems_rejects_duplicate_comids():
    duplicate_lookup = lookup()
    duplicate_lookup.loc[1, "comid"] = 101

    with pytest.raises(ValueError, match="more than one mainstem"):
        join_catchments_to_mainstems(catchments(), duplicate_lookup, mainstems())


def test_join_catchments_to_mainstems_rejects_non_integer_featureids():
    fractional = catchments()
    fractional.loc[0, "featureid"] = 101.5

    with pytest.raises(ValueError, match="integer comid"):
        join_catchments_to_mainstems(fractional, lookup(), mainstems())
