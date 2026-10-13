---
name: register-catalog
description: Register a Portolan catalog in the Portolan registry by opening a pull request that adds a catalog entry file.
---

<!-- drift: depends-on: portolan-registry, rashid -->

# Register a Catalog in the Portolan Registry

You are helping a user add their Portolan catalog to the [portolan-registry](https://github.com/portolan-sdi/portolan-registry) by opening a pull request. The registry crawls and validates submitted catalogs, then exports their metadata. A Portolan catalog is a STAC catalog that follows the Portolan specification.

## Key fact: submitters provide a URL and an address

A registry entry is one YAML file with two fields, both required:

```yaml
url: https://example.com/stac/catalog.json
submitter_email: you@example.org
```

CI extracts everything else by crawling the catalog: title, description, bbox, license, counts, the schema URI, the `agents` and `describedby` links, the `icon` link, and `providers`. Never add other fields or invent metadata. The [schema](https://github.com/portolan-sdi/portolan-registry/blob/main/schema/entry.schema.json) sets `additionalProperties: false`, so CI rejects an extra field. An entry that lacks either required field fails the registry check.

## Step 1: Validate the catalog

The URL must end in `catalog.json` and point to a reachable Portolan catalog root. Check the local tree first, then check the published URL before registration. `rashid check` accepts a local path or the HTTPS URL of a published catalog.

```bash
uvx --from 'rashid>=0.3.0,<0.4.0' rashid check ./catalog --summary
uvx --from 'rashid>=0.3.0,<0.4.0' rashid check "$CATALOG_URL" --summary
curl -fsSL "$CATALOG_URL" | jq '{type, id, stac_extensions}'
```

Stop and tell the user when the URL fails any of these checks. It must end in `catalog.json`. It must be reachable. It must return `"type": "Catalog"`. If rashid reports errors, show them to the user and let them decide whether to fix the catalog first. The registry mails the submitter when a registered catalog stops validating.

## Step 2: Choose the slug

The slug is the stem of the entry file, `catalogs/$SLUG.yaml`, and becomes the registry id. The registry has no naming rule. Pick a short, lower-case, hyphenated name that identifies the catalog. The directory that holds `catalog.json` is a good default.

```bash
# .../nlebovits/pergamino-ide/catalog.json  ->  pergamino-ide
SLUG=$(basename "$(dirname "$CATALOG_URL")")
```

Check that the slug is free. The registry rejects a duplicate URL, but it does not detect a reused stem. A colliding stem overwrites another entry.

```bash
if curl -fsSL "https://raw.githubusercontent.com/portolan-sdi/portolan-registry/main/catalogs/$SLUG.yaml" >/dev/null 2>&1; then
  echo "catalogs/$SLUG.yaml exists. Choose another slug."
fi
```

## Step 3: Ask the user for the submitter address

Ask the user which address to record, and wait for an answer. Never guess one, and never read it out of git config. The person answerable for a registration is not always the person who runs the command.

Tell them what the address is for. The registry mails it when the catalog stops validating, and when someone files feedback against it. It stays in `catalogs/` and never reaches `exports/catalogs.json`.

```bash
SUBMITTER_EMAIL="you@example.org"  # supplied by the user
```

## Step 4: Open the PR

Fork, clone into a temporary directory, add the file, and open the PR. Write the PR body first, in Simplified Technical English. The registry runs `lint_body.py --kind pr`, which requires the three sections and the waiver line below.

<!-- drift-sample: pr -->
````markdown
## What changed

Adds `catalogs/pergamino-ide.yaml`. The registry crawls
`https://data.source.coop/nlebovits/pergamino-ide/catalog.json` on the next run.

## Why

The catalog is public and passes `rashid check`. Registration lists it in
the registry export and on the website.

## Verification

```
$ uvx --from 'rashid>=0.3.0,<0.4.0' rashid check ./catalog --summary
OK: 14 files checked, no findings.
$ curl -fsSL https://data.source.coop/nlebovits/pergamino-ide/catalog.json | jq -c '{type, id}'
{"type":"Catalog","id":"pergamino-ide"}
```

- [x] This change does not alter behavior (docs, chore, or CI only).
````

Then run the commands. `CATALOG_URL`, `SLUG`, and `SUBMITTER_EMAIL` carry over from the steps above. Save the body as `body.md` in the working directory.

```bash
TMP=$(mktemp -d)
gh repo fork portolan-sdi/portolan-registry --clone "$TMP/portolan-registry"
cd "$TMP/portolan-registry"

git checkout -b "feat/register-$SLUG"
printf 'url: %s\nsubmitter_email: %s\n' \
  "$CATALOG_URL" "$SUBMITTER_EMAIL" > "catalogs/$SLUG.yaml"
git add "catalogs/$SLUG.yaml"
git commit -m "feat: register $SLUG"
git push -u origin "feat/register-$SLUG"

gh pr create \
  --repo portolan-sdi/portolan-registry \
  --title "feat: register $SLUG" \
  --body-file "$OLDPWD/body.md"
```

The title is in conventional-commit form because squash-merge makes it the commit message.

## Step 5: Report

Give the user the PR URL. The `Validate Catalogs` workflow mirrors new catalog metadata and runs rashid and stac-node-validator. Changes to current entries require a maintainer approval of the head commit. A PR that only adds new entries merges on its own when that workflow passes. After the merge, `publish.yml` opens a bot PR that updates `exports/catalogs.json`.

## Alternative: web submission

If the user prefers not to use GitHub, the web form at [portolan-sdi.org](https://www.portolan-sdi.org) takes the same two values: the `catalog.json` URL and an email address.
