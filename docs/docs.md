gleaner processs

- takes an individual config and gets jsonld for each site


nabu release --cfg <cfgpath> --prefix prov/refgages0  

creates s3:/myminio/iow/graphs/archive/refgages0/summoned__refgages0_2024-07-08-21-49-19_release.nq and copies to s3:/myminio/iow/graphs/latest/refgages0_release.nq


- construct an nq file from all of the jsonld produced by gleaner



nabu --cfg <cfg> --endpoint iow_data object "/graphs/latest/refgages0_release.nq"

no changes  occur to s3.  7193 should be count of triples in 'iow_data' repo (per cfg) after this cmd. src .nq file is 7193 (some triples are from base ontologties (70) and presumably rest inferred triples"


take the nq file from the s3 and use the sparql api to upload the nq object into the graph



nabu --cfg <cfg> prune --prefix "summoned/refgages0" 

s3 does not change. graph triple count should not have changed (nothing to prune?). Will have to see if this behavior changes when running same dataset subsequent times to test for idempotency

This is like a diff that upadtes the graph. it is supposed to look at the prefix directory path and reconciles the files that exist with what exists in the graph. if there are any triples in the graph not in the s3, they will be deleted
s3 is the source of truth
if the graph is missing data it should be added


nabu --cfg <cfg> prefix --endpoint iow_prov  --prefix "prov/refgages0" 

s3 remains unchanged. (iow_prov) graph should have 4620 triples

similar to object but instead of looking for nq it looks for jsonld that are instances of the prov ontology and puts those into the graph
prov ontology is intended to establish data lineage. (metadata about construct of the final graph) We use both prov and object since they are orthogonal. 
put in a different repository; didn't want to pollute the main graph

nabu --cfg <cfg> prefix --prefix "orgs" 

no change to s3. expect 4660 triples in 'prov' repo which was previously 4620 before run ( 40 new  triples from s3:/myminio/orgs/ which contains one .nq file for each source org harvested)

loads jsonld from the orgs directory in s3
loads info about the organization that provided the jsonld. flattened.
doesn't use prune. just a quirk of nabu
someting to test to see if it syncs