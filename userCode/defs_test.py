# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

import gzip
from pathlib import Path
import shutil

from dagster import (
    AssetSpec,
    AssetsDefinition,
    DagsterInstance,
    SourceAsset,
    load_assets_from_modules,
    materialize,
)
from rdflib import Dataset, URIRef

from test.lib import SparqlClient, assert_rclone_config_is_accessible
from userCode.assetGroups.harvest import (
    EXIT_3_IS_FATAL,
    sources_partitions_def,
)
from userCode.assetGroups.index_generator import (
    GEOCONNEX_GRAPH_DIRECTORY,
    pull_release_nq_for_all_sources,
)
from userCode.assetGroups.release_graph_generator import (
    ADD_MAINSTEM_INFO_TAG,
    MAINSTEM_FILE_OVERRIDE_TAG,
)
import userCode.defs as defs
from userCode.lib.classes import S3
from userCode.lib.env import S3_DEFAULT_BUCKET


def assert_data_is_linked_in_graph():
    """Check that a mainstem is associated with a monitoring location in the graph"""
    query = """
    PREFIX schema: <https://schema.org/>

    SELECT * WHERE {
     ?subject schema:name "FLORIDA FARMERS CANAL" .
    } LIMIT 100
    """

    resultDict = SparqlClient().execute_sparql(query)
    assert len(resultDict["subject"]) >= 1, (
        "Could not find the Florida Canal in the graph"
    )

    query = """
    PREFIX hyf: <https://www.opengis.net/def/schema/hy_features/hyf/>

    select * where { 
        ?monitoringLocation hyf:referencedPosition/hyf:HY_IndirectPosition/hyf:linearElement <https://geoconnex.us/ref/mainstems/42750> .
    } limit 100 
    """
    resultDict = SparqlClient().execute_sparql(query)
    # make sure that the florida canal monitoring location is on the florida river mainstem
    assert len(resultDict["monitoringLocation"]) > 0, (
        "There were no linked monitoring locations for the Florida River Mainstem"
    )
    assert (
        "https://pids.geoconnex.dev/cdss/gages/FLOCANCO"
        in resultDict["monitoringLocation"]
    )


def test_e2e_sync():
    """Run the e2e test by syncing against a local graphdb graph"""
    SparqlClient("iow").clear_graph()
    SparqlClient("iowprov").clear_graph()
    # insert a dummy graph before running that should be dropped after syncing ref_mainstems_mainstems__0
    SparqlClient().insert_triples_as_graph(
        "urn:iow:summoned:ref_mainstems_mainstems__0:DUMMY_PREFIX_TO_DROP",
        """
        <http://example.org/resource/1> <http://example.org/property/name> "Alice" .
        <http://example.org/resource/2> <http://example.org/property/name> "Bob" .
        """,
    )

    instance = DagsterInstance.ephemeral()

    assert (
        defs.defs.get_job_def("setup_config")
        .execute_in_process(instance=instance)
        .success
    )

    all_partitions = sources_partitions_def.get_partition_keys(
        dynamic_partitions_store=instance
    )
    assert len(all_partitions) > 0, "Partitions were not generated"

    assert (
        defs.defs.get_job_def("harvest_and_sync")
        .execute_in_process(
            instance=instance,
            tags={EXIT_3_IS_FATAL: str(True)},
            partition_key="ref_mainstems_mainstems__0",
        )
        .success
    ), "Job execution failed for partition 'mainstems__0'"

    objects_query = """
    select * where {
        <https://pids.geoconnex.dev/ref/mainstems/42750> <https://schema.org/name> ?o .
    } limit 100
    """

    resultDict = SparqlClient().execute_sparql(objects_query)
    assert "Florida River" in resultDict["o"], (
        "The Florida River Mainstem was not found in the graph"
    )

    assert (
        defs.defs.get_job_def("harvest_and_sync")
        .execute_in_process(instance=instance, partition_key="cdss_co_gages__0")
        .success
    ), "Job execution failed for partition 'cdss_co_gages__0'"

    assert_data_is_linked_in_graph()
    # Don't want to actually transfer the file but should check it is installed
    assert_rclone_config_is_accessible()

    assert (
        defs.defs.get_job_def("export_nquads")
        .execute_in_process(instance=instance)
        .success
    )

    all_graphs = SparqlClient().execute_sparql("""
    SELECT DISTINCT ?g 
    WHERE { 
        GRAPH ?g { 
            ?s ?p ?o .
        } 
    }
    """)
    # make sure we have 2 orgs graphs since we crawled 2 sources so far
    # urn:iow:orgs is nabu's way of serializing the s3 prefix 'orgs/'
    NUM_ORG_GRAPHS = sum("urn:iow:orgs" in g for g in all_graphs["g"])
    assert NUM_ORG_GRAPHS == 2
    assert not any("DUMMY_PREFIX_TO_DROP" in g for g in all_graphs["g"]), (
        "The dummy graph we inserted crawling was not dropped correctly"
    )

    # make sure that prov graphs were generated for the mainstem run
    mainstem_prov_graphs = SparqlClient(repository="iowprov").execute_sparql("""
        SELECT DISTINCT ?g
        WHERE {
        GRAPH ?g {
            ?s ?p ?o .
        }
        FILTER(CONTAINS(STR(?g), "urn:iow:prov:ref_mainstems_mainstems__0"))
        }
        """)
    assert len(mainstem_prov_graphs["g"]) > 0, (
        "prov graphs were not generated for the mainstem run"
    )


def test_e2e_harvest_and_release_nquads():
    """Run the e2e test for harvesting and releasing the nquads with mainstem info"""
    # clear any previous graphs to ensure a clean slate
    S3().remove_prefix("graphs/latest")

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
        defs.defs.get_job_def("harvest_and_release_as_nq")
        .execute_in_process(
            instance=instance,
            tags={
                EXIT_3_IS_FATAL: str(True),
                ADD_MAINSTEM_INFO_TAG: str(True),
                MAINSTEM_FILE_OVERRIDE_TAG: str(test_flatgeobuf),
            },
            partition_key="ref_dams_dams__0",
        )
        .success
    ), "Job execution failed for partition 'ref_dams_dams__0'"

    obj = S3().client.get_object(
        S3_DEFAULT_BUCKET, "graphs/latest/ref_dams_dams__0_release.nq.gz"
    )
    with gzip.GzipFile(fileobj=obj) as gz:
        data = gz.read()

    text = data.decode("utf-8")

    assert (
        "<https://www.opengis.net/def/schema/hy_features/hyf/linearElement> <https://reference.geoconnex.us/collections/mainstems/items/36825>"
        in text
    ), (
        "Mainstem info should have been inserted into the nquads during converstion. The mainstem should be associated with https://features.geoconnex.dev/collections/dams/items/1076356"
    )
    if GEOCONNEX_GRAPH_DIRECTORY.exists():
        shutil.rmtree(GEOCONNEX_GRAPH_DIRECTORY)
    pull_release_nq_for_all_sources()
    assert GEOCONNEX_GRAPH_DIRECTORY.exists(), "Pulled nq folder does not exist"

    pulled_file = GEOCONNEX_GRAPH_DIRECTORY.joinpath("ref_dams_dams__0_release.nq.gz")
    last_modified = pulled_file.stat().st_mtime
    pulled_file_bytesum = GEOCONNEX_GRAPH_DIRECTORY.joinpath(
        "ref_dams_dams__0_release.nq.gz.bytesum"
    )
    assert pulled_file.exists()
    assert pulled_file_bytesum.exists()

    pull_release_nq_for_all_sources()

    assert last_modified == pulled_file.stat().st_mtime, (
        "Since the bytesum is the same there should be no new pull and no new data transferred to disk"
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
    assert "ref_mainstems_mainstems__0" in newPartitions
    assert "ref_dams_dams__0" in newPartitions

    # Check what happens when we delete a specific key in the dynamic partition
    instance.delete_dynamic_partition(
        "sources_partitions_def", "ref_mainstems_mainstems__0"
    )

    # Make sure that partitions are deleted
    partitionsAfterDelete = instance.get_dynamic_partitions("sources_partitions_def")
    assert "ref_mainstems_mainstems__0" not in partitionsAfterDelete
    assert "ref_dams_dams__0" in partitionsAfterDelete
    assert len(partitionsAfterDelete) == len(newPartitions) - 1
    for key in mocked_partition_keys:
        assert key not in partitionsAfterDelete
