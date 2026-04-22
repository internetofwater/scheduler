-- Copyright 2026 Lincoln Institute of Land Policy
-- SPDX-License-Identifier: Apache-2.0


-- used to generate the geoconnex_features parquet subset for test data
INSTALL httpfs;
LOAD httpfs;

COPY (
    SELECT *
    FROM 'https://storage.googleapis.com/metadata-geoconnex-us/exports/geoconnex_features.parquet'
    LIMIT 5
) TO 'geoconnex_features_subset.parquet' (FORMAT PARQUET);