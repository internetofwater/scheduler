---
name: portolan-bootstrap
description: Build a complete, well-documented Portolan catalog from a data source. Research the data and its publisher, convert to cloud-native formats, write the documentation and styles that make it usable, and publish. Use when someone wants to publish, mirror, or 'portolan-ify' a dataset, an open data portal, an ArcGIS or WFS service, or a folder of geospatial files.
---

<!-- drift: depends-on: portolan-cli, portolan-spec -->

# Portolan Bootstrap

This skill takes the data-first path. You hold the files. You convert and document them. Then you publish the whole catalog to a bucket.

If the user wants catalog metadata to live in a git repository, validated by CI and open to pull requests, use the `git-backed-catalog` skill instead.

If a catalog or a published dataset already exists and the job is to bring it into compliance, use the `portolan-migrate` skill. It audits what is there and repairs it in place.

## The Goal

A finished catalog meets three standards. Someone who has never seen the data can decide in one screen whether to trust it. An agent can write a correct query on the first try. Every collection renders a readable map the moment it opens.

Validation is the floor, not the goal. A catalog can pass every check and still tell a reader nothing. That catalog is not done.

Read an exemplar before you start. [portolan-nl](https://source.coop/cholmes/portolan-nl) is a published catalog that meets the three standards. The [portolan-reference catalog](https://github.com/portolan-sdi/portolan-spec/tree/main/examples/catalog/portolan-reference) in portolan-spec is the annotated minimum.

The [spec](https://github.com/portolan-sdi/portolan-spec) is the standard. The CLI implements it and is one way to get there, not the definition of done. Where a better tool fits a step, use it:

- [gpio](https://github.com/developmentseed/geoparquet-io) for conversion and spatial sorting
- tippecanoe directly for tiling options the CLI does not expose
- DuckDB for profiling a column before you style it

Record what you ran in the collection's `AGENTS.md`.

Read [philosophy.md](https://github.com/portolan-sdi/portolan-spec/blob/main/specs/best-practices/philosophy.md) for what the standard is buying.

## Working Agreement

**Checkpoint when uncertain.** Asking costs a minute. Guessing costs a republish.

**Preview before anything expensive.** `extract` and `push` take `--dry-run`. `check --fix --dry-run` previews conversion. `add --pmtiles` is the tiling path and has no dry run, so size the job from the extraction report before you tile. Report the numbers before you spend the time.

**Warn inline, summarize at the end.** A failed layer does not stop the run. It also does not disappear from the final report.

**Front-load research and quality control.** A rendering that lies costs more to fix than to prevent. The user finds it on a screenshot after publication.

**Fix upstream faults upstream.** When the browser, the CLI, or the spec is wrong, file the issue there. A workaround here becomes a defect every other catalog inherits.

**Reuse the publisher's vocabulary.** Their topics, tags, and department names are what the data's audience already searches for.

**Publish when you can defend it.** Every collection should answer the question "where did this sentence come from, and does that map show what the legend claims?"

## Record the source of every claim

Research widely. Then make each claim traceable. A fact belongs in the catalog when it fits one of three tiers.

**Attested.** The source states it in its service metadata, a GetCapabilities document, the portal's dataset page, or a published data dictionary. Copy the wording for license, attribution, and legal statements. Record where it came from.

**Researched and cited.** The claim comes from outside the source's own metadata. Look in the publisher's data dictionary PDF, an agency program page, a standard's code list, or the ordinance that created the program. This tier is encouraged. The condition is a link. Record the URL in the collection's `processing_notes`, and as a markdown link in the description or `AGENTS.md` where a reader benefits.

**Derived.** The number comes from the published data. Examples are a row count, a value distribution, and a measure like sale price per square foot. Put the query in `AGENTS.md` so the reader can rerun it. Never state a derived number you did not compute.

Anything else is invention, including the plausible kind. Guessing that pipe-material code 109 means ductile iron is invention. Finding the utility's code list and citing it is research. Reporting that 109 is the most common value is derivation.

When research fails, say so. "The publisher has not released a code list for `MATERIAL`. The values are opaque integers" is honest and useful. A column left undescribed is a smaller failure than a column described wrongly. Record such gaps in `known_issues` in `.portolan/metadata.yaml`.

### Provenance fields

The spec derives whether a catalog is official or a mirror from `providers`. Every collection MUST list `providers` with at least one `producer` and exactly one `host`, listed last (PORTO-CORE-046, PORTO-CORE-047). The host MUST carry a `url` or an `email` (PORTO-CORE-051). When producer and host differ, the collection is a mirror. A mirror MUST carry a `via` link of type `text/html` to the original source (PORTO-CORE-053) and a top-level `updated` field set to the time of the sync (PORTO-CORE-057).

Declare the providers and the source in `.portolan/metadata.yaml`. The CLI writes the `via` link from `source_url` and sets `updated` when the providers make the collection a mirror. `portolan extract` also writes a `via` link on each collection it creates.

```yaml
providers:
  - name: "City of Example, Assessor's Office"
    roles: ["producer", "licensor"]
    url: "https://example.gov/assessor"
  - name: "Example Data Cooperative"
    roles: ["host"]
    url: "https://example.org/contact"
source_url: "https://example.gov/data/parcels"
attribution: "City of Example, Assessor's Office"
processing_notes: >
  Extracted from the ArcGIS FeatureServer on
  2026-08-12. MATERIAL codes decoded from
  https://example.gov/water/pipe-codes.pdf.
  Price per square foot derived from SALE_PRICE
  and SHAPE_Area. See AGENTS.md for the query.
```

When the source publishes its own STAC catalog, add a `canonical` link to that STAC root (PORTO-CORE-054). `portolan check` cannot know whether such a catalog exists, so this is your research finding, not a validator result.

**The test.** Before publishing, take any sentence from a description, a README, or an `AGENTS.md` and answer two questions: which tier is this, and where is its source? Do not publish a sentence with no answer.

Title, license, and contact never come from research alone. They stay checkpointed. You may propose a license you found on the publisher's terms page. The user confirms it.

Translation is checkpointed too. Do not translate a title or description unless the source publishes both languages or the user asks. The spec models a translation as a separate STAC tree per language, linked from the root with an `alternate` link (PORTO-CORE-079). A translated tree is never a `child`. Read [multilingual-catalogs.md](https://github.com/portolan-sdi/portolan-spec/blob/main/specs/best-practices/multilingual-catalogs.md) before you build one.

## Research the Data Before You Convert It

Do this before extraction. What you learn here decides which datasets are worth mirroring. It also decides which columns need decoding and which styles will say anything.

Work these targets in order.

**1. The portal dataset page and its siblings.** Read the page for each candidate dataset, plus the portal's own topic and tag structure. Publishers explain a dataset in prose on the portal that the service metadata omits.

**2. The service's own metadata.** For ArcGIS, fetch the service and layer JSON:

```bash
curl -s "<SERVICE_URL>/0?f=json" | jq '.fields'
```

Field aliases and `domains` carry the coded-value lists that decode integer columns. For WFS, read the capabilities and schema documents:

```bash
curl -s "<WFS_URL>?service=WFS\
&request=GetCapabilities"
```

`portolan extract <arcgis|wfs|carto> <URL> --dry-run` lists layers or tables without downloading anything. Use it here to size the job.

**3. Published data dictionaries and metadata files.** Attached PDFs, FGDC or ISO XML, and the `.txt` readme inside a shapefile download are the usual home of column meanings.

**4. The publishing department and the program.** Which office publishes a dataset explains update cadence, coverage gaps, and what the rows count. This is also where the `producer` provider comes from.

**5. The licensing terms page.** Find the terms that apply to this dataset, not the portal-wide default.

**6. Sibling portals.** Many agencies also run an ArcGIS Online gallery that holds layers the open data portal does not list. Check it for duplicates and additions, and say which is which.

**7. Existing publisher cartography.** SLD files, style JSON, and tile servers let you match the publisher's own colors. Save them. They are evidence for the colors you choose, and they are not Portolan styles. The `style` role is for the MapLibre style files you publish (PORTO-CORE-069).

Record findings per candidate as you go:

- what the dataset is
- who publishes it
- what each non-obvious column means, and where that meaning came from
- what remains unclear
- whether it duplicates another candidate

### Assess the Mirror Path

Decide before conversion whether to build a metadata-only mirror or a full mirror.

A metadata-only mirror points its `data` assets at the upstream copy. A full mirror contains spec-compliant copies that you host. The Data Storage requirements apply to servers that host the catalog's own assets. They do not apply to upstream servers, and a validator MUST NOT require upstream servers to meet them (PORTO-CORE-073). A weak upstream limits what clients can do with that copy. It does not make the catalog non-conformant.

So the probe measures capability. Run it on representative assets:

```bash
skills/portolan-bootstrap/scripts/probe-upstream.sh \
  "$ASSET_URL"
```

Read the HTTP version, status, `Accept-Ranges`, `Content-Range`, total size, and transferred byte count. Compare the HEAD `Content-Length` with the `Content-Range` total. Do not use a ranged HEAD as proof of ranged GET behavior. Compare the CORS response with the Data Storage section of [core.md](https://github.com/portolan-sdi/portolan-spec/blob/main/specs/portolan/core.md#data-storage).

For an archive, run `unzip -v` and distinguish bare files, stored members, and compressed members. A compressed member blocks selective reads of its inner file. For a GeoTIFF, run `gdalinfo` against `/vsicurl/$ASSET_URL` with `GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR` and check tile blocks, overviews, and `LAYOUT=COG`.

Record observed values in a table. Put size, time, and maintenance estimates in a separate table. Then choose:

* Metadata-only mirror when the upstream copy is cloud-native and the probes show range and CORS support. Clients get the full capability set with no hosted bytes.
* Full mirror when the upstream copy is not cloud-native, or when the probes show a limit the users of this catalog will hit. Estimate recurring checks, refreshes, storage, transfer, and conversion.

When a small upstream change would remove the need for hosted copies, offer an optional `upstream-gaps.md` report. Do not block publication on it.

### Checkpoint: Research Findings

Present this before extracting anything:

* The candidate list with a one-line description each, and which ones you propose to drop as duplicative or low-value.
* Columns you decoded, with the source for each.
* Columns that remain opaque after research.
* Proposed license, producer, and attribution, with the page you found them on.
* Estimated feature counts, total size, and time.
* The selected mirror path, probe evidence, and expected maintenance cost.
* The optional `upstream-gaps.md` offer, when a small change can enable direct access.

Ask whether to proceed with this candidate set.

## Collection layout

The layout decision is the one the validator cannot make for you. A catalog with no item JSON at all can pass `portolan check`. A human found one such defect in a bucket listing, after a 619-scene collection was published as bare COGs under one directory with no items.

**Single file.** A collection with one GeoParquet or one COG exposes it as a collection-level asset with no item directory (PORTO-CORE-017, PORTO-CORE-072). Move any file at the catalog root into a named directory before you add it. `portolan add` writes a `catalog.json` at each intermediate level. A collection never contains a child collection (PORTO-CORE-014). A collection with more than one scene uses the multi-scene layout below.

**Multi-scene raster.** A collection with more than one raster scene MUST model each scene as an item. The item exposes its COG as an item-level asset (PORTO-CORE-071). A collection directory contains one subdirectory per item (PORTO-CORE-015). `portolan add` derives the item id from the parent directory name, so lay the files out that way before you add them:

```text
population/
├── collection.json
├── items.parquet             # derived, role collection-mirror
├── styles/
├── 1975/
│   ├── 1975.json
│   └── 1975.tif
└── 1990/
    ├── 1990.json
    └── 1990.tif
```

Such a collection SHOULD also publish `items.parquet` in the collection root (PORTO-FMT-040). Register it as a collection asset with media type `application/vnd.apache.parquet` and the role `collection-mirror` (PORTO-FMT-041). It is a derived copy. The item JSON remains normative, and the mirror MUST reproduce every item at publication time (PORTO-FMT-042). The mirror never replaces item JSON or the collection's `item` links. Generate it with `portolan add --stac-geoparquet` or `portolan stac-geoparquet`. The rule applies at any item count.

**Many children.** A catalog or collection with twenty or more children SHOULD group them into subcatalogs (PORTO-CORE-078). Group by the publisher's own topics or by year, never by a taxonomy you invented.

## The Documentation Contract

The standards live in [documentation.md](https://github.com/portolan-sdi/portolan-spec/blob/main/specs/best-practices/documentation.md) and the scoring in [grader.md](https://github.com/portolan-sdi/portolan-spec/blob/main/specs/best-practices/grader.md). Read both. This section covers only what agents reliably get wrong.

Each catalog and collection includes an `AGENTS.md` linked with `rel: agents` and a `README.md` linked with `rel: describedby` (PORTO-CORE-005, PORTO-CORE-061, PORTO-CORE-062). The CLI writes both links.

### STAC Descriptions

Every URL is a markdown link. A bare URL renders as plain text in the data browser. This includes URLs inside text copied from the source, which is where most of them come from.

```markdown
File a new service request at
https://example.gov/csb/submit        <- renders as text

File a new [service request](https://example.gov/csb/submit).
```

Mention the agent guide in the prose with an inline `[AGENTS.md](AGENTS.md)` link. A trailing "Start at the catalog agent guide" reads as boilerplate and gets skipped.

Call the browser page the "data browser" or the "interactive data page". It is not an "interactive map". It is also where a reader downloads the data. The same page shows the license, the schema, and the outbound links.

Do not link a collection description to itself.

### README.md

Generated output, never hand-edited. Edit `.portolan/metadata.yaml` and regenerate with `portolan readme`. A hand-edited README is replaced on the next run. The README MUST carry a title, description, license, and data provenance (PORTO-CORE-063). The provenance section comes from `source_url` and `processing_notes`.

### AGENTS.md

The CLI scaffolds a stub and never overwrites an existing file. Replacing the stub prose is your job. An unedited stub is a published defect, and validation will not catch it.

Write the join keys, the CRS and what it costs a consumer, the quirks a query will hit, and recipes that run. When a join connects a tabular dataset to the map, the join belongs here as a query someone can paste:

```sql
-- Sales joined to parcel geometry on parcel id
SELECT s.sale_price, s.sale_date, p.geometry
FROM read_parquet('sales.parquet') s
JOIN read_parquet('parcels.parquet') p
  ON s.parcel_id = p.parcel_id;
```

Every query in `AGENTS.md` must have been run against the published data. A recipe that fails on the first try is worse than no recipe.

### Column Descriptions

`table:columns` is where researched meaning lands (PORTO-FMT-046). A coded column copied through with no decoded meaning is unfinished work, whether or not the catalog validates. `MATERIAL` described as "material" adds nothing.

### Link Hygiene

Use `source.coop` URLs for anything a human reads, because that host renders READMEs and agent guides. Use `data.source.coop` only for raw byte fetches by a machine. Thumbnails, README links, and "additional resources" lists all use the wrong host by default. The `sourcecoop` skill is the canonical home for this rule.

## Styles That Say Something

Read [styling.md](https://github.com/portolan-sdi/portolan-spec/blob/main/specs/best-practices/styling.md) and the Visualization section of [core.md](https://github.com/portolan-sdi/portolan-spec/blob/main/specs/portolan/core.md#visualization) for the requirements.

Each style is a collection-level asset with media type `application/vnd.mapbox.style+json` and the `style` role (PORTO-FMT-015, PORTO-CORE-069). When a collection ships more than one style, exactly one carries both `style` and `default` (PORTO-CORE-070). There is no manifest and no registration step beyond the asset entry. The PMTiles file itself is a collection-level `rel: pmtiles` link that carries a non-empty `pmtiles:layers` array (PORTO-FMT-011, PORTO-FMT-012).  `portolan add --pmtiles` writes that link.

Publish as many styles as the data supports distinct readings of, and no more. A style that paints every feature the same color says nothing the bounding box did not already say. Choose styles after you query the actual distributions. Sale price plus area becomes price per square foot. Use a date column as an age band or a recency band. Style a category column only after you confirm the categories are populated and mean something.

Name styles in the reader's language. Vary palettes across sibling collections. Sibling datasets in the same pale blue read as one dataset in a card grid.

### The Legend Rule

As of 2026-08-12, the browser derives a legend only from a `fill` layer whose `fill-color` is a `match` or `step` expression. An `interpolate` or `case` expression yields no legend. Line, circle, and symbol layers yield no legend. See [portolanStyles.js](https://github.com/portolan-sdi/portolan-browser/blob/main/src/utils/portolanStyles.js) for current behavior.

A style that produces a legend looks like this:

```json
{
  "type": "fill",
  "paint": {
    "fill-color": [
      "match", ["get", "material"],
      "ductile_iron", "#1f77b4",
      "lead", "#d62728",
      "#cccccc"
    ]
  }
}
```

A line or point dataset gets no legend from its natural layer type. Plan its styles knowing that.

Every `match` branch must be checked against real values before publishing. A legend listing ten categories over a map painted one color is the most common visible defect in a new catalog. Verify with a query:

```sql
SELECT category, count(*)
FROM read_parquet('data.parquet')
GROUP BY category
ORDER BY 2 DESC;
```

Every branch in the `match` should appear in that result, and every populated category worth showing should appear in the `match`.

### Checkpoint: Style Plan

Per collection, present the proposed styles, the column each uses, the distribution you measured, and whether a legend will appear. Ask for approval before generating tiles.

## Assets and Provenance

A reader who wants the authoritative original should not have to leave the catalog and search for it. A reader checking your work needs to see what you started from. Each carried asset has a role that says what it is:

* The upstream download you converted from has the `source` role when it is directly downloadable (PORTO-FMT-002). Link it at its original location. You do not need to rehost it, and its format is exempt from the format rules (PORTO-FMT-045).
* Sidecar metadata such as FGDC XML or a data dictionary PDF carries the `metadata` role. An ISO 19115 file carries `iso-19115` (PORTO-CORE-027, PORTO-FMT-003).
* A publisher logo is not an asset. Publish it with `portolan logo <file>`, which writes a root `rel: icon` link (PORTO-CORE-074). Only do this when the user confirms the catalog may carry the publisher's branding.

Give each carried asset a title that says what it is and where it came from, so the asset list reads as a provenance record rather than a list of filenames.

When the source is a feature service or a WFS endpoint rather than a file, the `via` link covers the endpoint. Layer selection, pagination, and any bbox filter belong in `processing_notes`.

The extraction date is written to `updated` on each mirrored catalog and collection (PORTO-CORE-057). Confirm it after `add` and after every refresh. A mirror without a date cannot be compared to the live service.

Keep GeoParquet in the source CRS. Reprojection belongs in the PMTiles, which need Web Mercator to render. A consumer who needs native coordinates cannot recover them from a reprojected file.

Drop `lat` and `lon` columns when a geometry column has the same information. They survive extraction from tabular sources and show up in `table:columns` as noise.

See [conversion-defaults.md](https://github.com/portolan-sdi/portolan-spec/blob/main/specs/best-practices/conversion-defaults.md) for format and compression defaults.

## Quality Control Before You Publish

Open each collection in the data browser and look at it. Most defects in a new catalog are visible, and validation catches none of them.

Per collection:

* The default style renders visible data at the full extent. A blank map or a barely-visible tint is a failure.
* Every legend entry corresponds to values that exist in the data.
* The bbox is tight. One outlier row dragging the extent into the next state makes every thumbnail useless.
* The first PMTiles load is small enough to open on a laptop, and the data is still complete at the target zoom.
* The thumbnail shows data rather than basemap. Hand off to the `portolan-thumbnails` skill when the generated thumbnail is not good enough.
* A multi-scene raster collection has one item directory per scene, and `items.parquet` has one row per item.
* Every link in the description, README, and `AGENTS.md` resolves, and human-facing links point at `source.coop`.

Check the bbox with a query, because one stray row is invisible at full extent and moves the whole frame:

```sql
SELECT count(*)
FROM read_parquet('data.parquet')
WHERE NOT ST_Within(
  geometry,
  ST_MakeEnvelope(-90.4, 38.5, -90.1, 38.8)
);
```

Check tile weight by opening the PMTiles in [pmtiles.io](https://pmtiles.io) and zooming. The first tile has to remain small, and the data still has to be complete at the zoom where people will look. Tightening one usually breaks the other, so confirm both after every tippecanoe change.

Then run the validator. Once the catalog is published, run it again with `--live` and pass the published base URL, so the probe covers your host and skips upstream hosts you do not control:

```bash
portolan check
portolan check --live --url "<PUBLISHED_URL>"
```

Score the result against [grader.md](https://github.com/portolan-sdi/portolan-spec/blob/main/specs/best-practices/grader.md) and report the tier per section at the final checkpoint.

Verify the catalog as a consumer would, using the `reading-portolan` skill. Run the queries you wrote into `AGENTS.md`, and fix the documentation when one of them does not run.

### Checkpoint: Publish

Present the file count, total size, destination, the grader tiers, and every warning accumulated during the run. Ask before pushing.

## Mechanics

The `portolan-cli` skill is the full reference. `portolan <cmd> --help` is the source for flags. This section covers the bootstrap path only.

Starting from a remote service. `extract` initializes the catalog unless you pass `--raw`. `arcgis` and `wfs` filter with `--layers` and `--exclude-layers`. `carto` filters with `--tables` and `--exclude-tables`:

```bash
portolan extract arcgis "<URL>" ./catalog \
  --dry-run
portolan extract arcgis "<URL>" ./catalog \
  --license CC-BY-4.0
cd catalog
portolan metadata init
portolan add . --pmtiles
portolan readme
```

Starting from a directory of files:

```bash
portolan init --license CC-BY-4.0
portolan metadata init
portolan scan --suggest-collections
portolan check --fix --dry-run
portolan check --fix
portolan add . --pmtiles --stac-geoparquet
portolan readme
```

Fill in `providers`, `source_url`, and `processing_notes` in `.portolan/metadata.yaml` before `portolan add`. `add` applies them to the collections it writes. When you edit the file later, `portolan add .` skips unchanged files and applies nothing. Rerun it with `--force` so the collections pick up the change, then run `portolan readme`.

Both paths then run through research, documentation, styles, and quality control before any push. `portolan push --dry-run` previews the upload.

Destination setup is delegated. Use the `sourcecoop` skill for Source Cooperative, which is the common case, and the `git-backed-catalog` skill when the metadata belongs in a repository. After publishing, use the `register-catalog` skill to add the catalog to the registry.

## Checkpoints and Failure Handling

| Checkpoint | Present | Ask |
|---|---|---|
| Research findings | Candidates, decoded columns, opaque columns, proposed license and producer | Proceed with this set? |
| Discovery | Layer or file count, feature estimates, size, time, warnings | Proceed with extraction? |
| Extraction complete | Success count, failures with reasons, total size | Continue? |
| Destination | Options available | Where should this publish? |
| License and contact | What the source states, and where it says it | Confirm license, host name, and url or email |
| Style plan | Styles per collection, columns, distributions, legend viability | Approve before tiling? |
| Quality control | Per-collection checklist results and grader tiers | Fix now, or publish? |
| Publish | File count, size, destination, accumulated warnings | Push? |

| Situation | Action |
|---|---|
| Layer fails extraction | Warn inline, continue, summarize at end |
| Missing CRS | Flag it, ask the user whether it is critical |
| Dataset over 100k features | Warn about time and memory before proceeding |
| Credentials invalid | Stop and help the user fix it |
| Upstream fails a range or CORS probe | Choose the full mirror for that asset. The catalog remains conformant either way |
| Mixed languages in source metadata | Checkpoint and ask which is primary |
| Field exists in source but was not extracted | Checkpoint with the exact source text |
| Required field missing from source | Checkpoint and ask the user to provide it |
| Producer unknown after research | Checkpoint. `providers` cannot be guessed |
| Column meaning not found after research | Document the gap in `known_issues`. Do not guess |
| Legend lists categories the data lacks | Fix the style before publishing, not after |
| Thumbnail shows basemap only | Hand off to `portolan-thumbnails` before publishing |
| Documented query fails when rerun | Fix the query or delete it. Do not publish it broken |
| Source file cannot be carried as an asset | Keep the `via` link and record why in `processing_notes` |
| Raster collection has scenes but no item directories | Relayout as one subdirectory per scene and re-add before publishing |
| Browser, CLI, or spec behaves wrongly | File the issue upstream. Note the workaround in `known_issues` |
