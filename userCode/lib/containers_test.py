# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: Apache-2.0

from userCode.lib.containers import SitemapHarvestConfig, SynchronizerConfig
from userCode.lib.env import (
    GLEANER_LOG_LEVEL,
    GLEANER_SITEMAP_WORKERS,
    NABU_BATCH_SIZE,
    NABU_LOG_LEVEL,
)


def test_default_value_of_config():
    """Make sure that the default values of the config match the env vars"""
    defaultSitemapConfig = SitemapHarvestConfig()
    assert defaultSitemapConfig.log_level == GLEANER_LOG_LEVEL
    assert defaultSitemapConfig.sitemap_workers == GLEANER_SITEMAP_WORKERS

    defaultSyncConfig = SynchronizerConfig()
    assert defaultSyncConfig.log_level == NABU_LOG_LEVEL
    assert defaultSyncConfig.upsertBatchSize == NABU_BATCH_SIZE
