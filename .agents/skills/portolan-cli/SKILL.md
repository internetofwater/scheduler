---
name: portolan-cli
description: Use when publishing, managing, or converting cloud-native geospatial data catalogs with the Portolan CLI. Covers init, add, check, push, pull, sync, partitioning, and format conversion workflows.
---

<!-- drift: depends-on: portolan-cli, portolan-spec, rashid -->

# Portolan CLI

Portolan is a specification for cloud-native geospatial data catalogs, built on STAC. `portolan-cli` implements it. The [portolan-spec](https://github.com/portolan-sdi/portolan-spec) repository is ground truth. Read `specs/portolan/core.md` and `specs/portolan/formats.md` there when a requirement is in doubt. The CLI converts data to GeoParquet, COG, and PMTiles. It then writes the STAC tree and validates it. `portolan push` syncs the result to object storage. There is no server. A catalog is static files.

This skill tracks the PyPI release named in `pins.toml`. Run `portolan <command> --help` for the options of any command. The help text is the reference. This skill only says which command to reach for and in what order.

## Installation

```bash
uv tool install portolan-cli
portolan --version
```

## Files the CLI writes

`portolan init` writes a root `catalog.json`, `AGENTS.md`, `README.md`, `versions.json`, and `.portolan/config.yaml` plus `.portolan/metadata.yaml`. The root declares the v0.2.0 schema URI in `stac_extensions`. That URI is the only signal of the spec version (PORTO-CORE-006). Its links are `root`, `agents`, and `describedby`. It writes no `self` link. Add an absolute `self` link at publish time (PORTO-CORE-081).

`portolan add <dir>` turns a directory into a collection. For one GeoParquet file, with `--pmtiles`, it writes:

```
demo/
├── collection.json
├── AGENTS.md
├── README.md
├── versions.json
├── data.parquet          asset role: data
├── data.pmtiles          asset role: visual, plus a rel: pmtiles link
├── data.thumb.jpg        asset role: thumbnail
└── styles/default.json   asset roles: style, default
```

Without `--pmtiles` there is no PMTiles file and no `styles/` directory, so a vector collection has no render path (PORTO-CORE-065). Pass `--pmtiles` for vector data.

`AGENTS.md` and `README.md` beside every `catalog.json` and `collection.json` are spec requirements (PORTO-CORE-005). `versions.json` is a CLI artifact that tracks sync state and checksums. It is not part of the spec. Dataset versioning uses the STAC version extension (PORTO-CORE-008).

One file is a collection-level asset with no item (PORTO-CORE-017). A partitioned collection uses the partition extension and its `partition:glob` (PORTO-FMT-017). Items for opaque partition schemes are not created (PORTO-FMT-022). Many raster scenes in one collection get one item each (PORTO-CORE-071), and `portolan stac-geoparquet` writes the `items.parquet` mirror for it.

Structural links stay relative, so the catalog is portable.

## Which command

| Task | Command |
|---|---|
| Start a catalog | `portolan init --license <SPDX>` |
| See what a directory contains before you add it | `portolan scan <dir>` |
| Add or update a collection | `portolan add <dir>` |
| Register remote data without copying it | `portolan add-external` |
| Validate against the spec | `portolan check` |
| Convert non-cloud-native files in place | `portolan check --fix` |
| Probe the published host for range requests and CORS | `portolan check --live --url <public base>` |
| Split a large GeoParquet file | `portolan partition` |
| Upload | `portolan push <remote>` |
| Download a remote catalog | `portolan pull <remote>` or `portolan clone <remote>` |
| Pull, check, and push one collection | `portolan sync <remote> -c <collection>` |
| Edit the metadata behind the READMEs | `portolan metadata init`, `portolan metadata validate` |
| Regenerate READMEs | `portolan readme` |
| Publish a logo | `portolan logo <file>` |
| Manage collection versions | `portolan version` |
| Read a skill from this repository | `portolan skills list`, `portolan skills show` |

Facts that trip agents:

- `init` needs `--license` whenever `--auto` or `--json` is passed. Without them it prompts.
- `sync` requires `-c`. It runs on one collection, never catalog-wide.
- `check` reads remote assets over range requests in its data pass. Pass `--data-scope local` to read only assets inside the tree, or `--no-data` to skip the pass.
- `check --fix` removes `portolan:datetime_provisional` from items. The item then has no provisional marker.
- `add` takes `--pmtiles`, `--force-pmtiles`, `--thumbnails`, `--force-thumbnails`, `--stac-geoparquet`, `--item-id`, `--datetime`, `--reconvert`, and `--force`. `--force-pmtiles` implies `--pmtiles`.
- `push --workers` is the parallelism across collections. `--concurrency` is the upload parallelism within one, default 8. Neither has a cap.
- The root group takes `--format json`. Most subcommands take `--json`. `partition` accepts no JSON flag.
- `check` calls the pinned `rashid`. Where the two disagree, the spec decides, and the disagreement is a bug to report.

## Workflows

Publish a new catalog:

```bash
portolan init --auto --license CC-BY-4.0 --title "My Geospatial Data"
portolan scan /data/geospatial
portolan add demographics/ --pmtiles
portolan check
portolan push s3://mybucket/my-catalog -c demographics
```

Update one collection end to end:

```bash
portolan sync s3://mybucket/my-catalog -c demographics --fix
```

Read the JSON envelope when a script runs the CLI:

```bash
portolan --format json check
portolan scan . --json
```

The envelope is `{"success": true, "command": "...", "data": {...}, "errors": []}`.

## Troubleshooting

| Error | Solution |
|-------|----------|
| "Not inside a Portolan catalog" | Run `portolan init` or move into the catalog. |
| `PRTLN-VAL004` | `init` or `add-external` ran with `--auto` and no `--license`. |
| "Push conflict: remote has newer version" | Run `portolan pull` first, or pass `--force` to overwrite. |
| Shapefile missing components | Provide `.shp`, `.shx`, and `.dbf` together. |
| Non-cloud-native files | Run `portolan check --fix` to convert vectors to GeoParquet and rasters to COG. |
