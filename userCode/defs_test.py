# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import gzip
from pathlib import Path

from dagster import (
    AssetSpec,
    AssetsDefinition,
    DagsterInstance,
    SourceAsset,
    load_assets_from_modules,
    materialize,
)
from rdflib import Dataset, URIRef

from userCode.assetGroups.export import (
    GEOCONNEX_GRAPH_DIRECTORY,
    features_from_harvested_parquet,
    nquads_for_all_sources,
    pull_harvested_parquet,
)
from userCode.assetGroups.harvest import (
    EXIT_3_IS_FATAL,
    MAINSTEM_FILE_OVERRIDE_TAG,
    sources_partitions_def,
)
import userCode.defs as defs
from userCode.lib.classes import S3
from userCode.lib.env import (
    HARVESTED_PARQUET_DIRECTORY,
    HARVESTED_PARQUET_PREFIX_IN_S3,
)


def test_e2e_harvest_and_generate_nquads():
    """Run the e2e test for harvesting to parquet and generating the nquads and geoparquet with mainstem info"""
    # clear any previous harvests to ensure a clean slate
    S3().remove_prefix(HARVESTED_PARQUET_PREFIX_IN_S3)

    instance = DagsterInstance.ephemeral()

    assert (
        defs.defs.get_job_def("setup_config")
        .execute_in_process(instance=instance)
        .success
    ), "Failed initializing configs for test run"

    all_partitions = sources_partitions_def.get_partition_keys(
        dynamic_partitions_store=instance
    )

    assert len(all_partitions) > 0, "Partitions were not generated"
    test_flatgeobuf = Path(__file__).parent / "testdata" / "colorado_subset.fgb"

    assert (
        defs.defs.get_job_def("harvest_and_generate_parquet")
        .execute_in_process(
            instance=instance,
            tags={
                EXIT_3_IS_FATAL: str(True),
                MAINSTEM_FILE_OVERRIDE_TAG: str(test_flatgeobuf),
            },
            partition_key="ref:dams",
        )
        .success
    ), "Job execution failed for partition 'ref:dams'"

    assert S3().object_has_content(
        f"{HARVESTED_PARQUET_PREFIX_IN_S3}ref:dams.parquet"
    ), "Harvest should have generated a parquet file in s3"

    nquads_for_all_sources()
    nquads_file = GEOCONNEX_GRAPH_DIRECTORY.joinpath("ref:dams.nq.gz")
    assert nquads_file.exists(), "Generated nquads file does not exist"

    with gzip.open(nquads_file) as gz:
        text = gz.read().decode("utf-8")

    assert (
        "<https://www.opengis.net/def/schema/hy_features/hyf/linearElement> <https://reference.geoconnex.us/collections/mainstems/items/36825>"
        in text
    ), (
        "Mainstem info should have been inserted into the jsonld during the harvest. The mainstem should be associated with https://features.geoconnex.dev/collections/dams/items/1076356"
    )

    ds = Dataset()
    ds.parse(data=text, format="nquads")
    assert len(ds) > 0

    pid_to_associated_mainstem = """
    PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>

    SELECT DISTINCT ?pid ?mainstem
    WHERE {
    GRAPH ?g {
        ?pid hyf:referencedPosition ?refPos .
        ?refPos hyf:HY_IndirectPosition ?indPos .
        ?indPos hyf:linearElement ?mainstem .
    }
    }
    ORDER BY ?mainstem
    """

    res = ds.query(pid_to_associated_mainstem)

    mainstems = {
        URIRef("https://pids.geoconnex.dev/ref/dams/1076356"): URIRef(
            "https://reference.geoconnex.us/collections/mainstems/items/36825"
        ),
        URIRef("https://pids.geoconnex.dev/ref/dams/1026348"): URIRef(
            "https://reference.geoconnex.us/collections/mainstems/items/35394"
        ),
    }
    for row in res.bindings:
        pid, mainstem = row["pid"], row["mainstem"]  # type: ignore rdflib does not have type hints properly
        assert mainstems[pid] == mainstem  # type: ignore rdflib does not have type hints properly

    pull_harvested_parquet()
    gdf = features_from_harvested_parquet(
        HARVESTED_PARQUET_DIRECTORY / "ref:dams.parquet", "ref:dams"
    )
    assert len(gdf) > 0
    assert set(gdf["geoconnex_sitemap"]) == {"ref:dams"}
    features = gdf.set_index("id")
    for pid, mainstem in mainstems.items():
        assert features.loc[str(pid), "mainstem_uri"] == str(mainstem)


def test_dynamic_partitions():
    """Make sure that a new materialization of the nabu config will create new partitions"""
    instance = DagsterInstance.ephemeral()
    mocked_partition_keys = ["test_partition1", "test_partition2", "test_partition3"]
    instance.add_dynamic_partitions(
        partitions_def_name="sources_partitions_def",
        partition_keys=list(mocked_partition_keys),
    )

    assert (
        instance.get_dynamic_partitions("sources_partitions_def")
        == mocked_partition_keys
    )

    assets = load_assets_from_modules([defs])
    # It is possible to load certain asset types that cannot be passed into
    # Materialize so we filter them to avoid a pyright type error
    filtered_assets = [
        asset
        for asset in assets
        if isinstance(asset, AssetsDefinition | AssetSpec | SourceAsset)
    ]
    # These three assets are needed to generate the dynamic partition.
    result = materialize(
        assets=filtered_assets,
        selection=["sitemap_partitions"],
        instance=instance,
    )
    assert result.success, "Expected gleaner config to materialize"

    assert (
        instance.get_dynamic_partitions("sources_partitions_def")
        != mocked_partition_keys
    )
    newPartitions = instance.get_dynamic_partitions("sources_partitions_def")

    # Make sure that the old partition keys aren't in the asset but
    # the new ones are
    for key in mocked_partition_keys:
        assert key not in newPartitions
    assert "ref:mainstems" in newPartitions
    assert "ref:dams" in newPartitions

    # Check what happens when we delete a specific key in the dynamic partition
    instance.delete_dynamic_partition("sources_partitions_def", "ref:mainstems")

    # Make sure that partitions are deleted
    partitionsAfterDelete = instance.get_dynamic_partitions("sources_partitions_def")
    assert "ref:mainstems" not in partitionsAfterDelete
    assert "ref:dams" in partitionsAfterDelete
    assert len(partitionsAfterDelete) == len(newPartitions) - 1
    for key in mocked_partition_keys:
        assert key not in partitionsAfterDelete
