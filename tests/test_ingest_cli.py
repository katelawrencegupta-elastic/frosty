"""Tests for frosty-ingest CLI configuration."""

from __future__ import annotations

import os
import unittest
from argparse import Namespace
from unittest import mock

from frosty.ingest import build_config


class IngestCliConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env = os.environ.copy()

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._env)

    def test_uses_env_api_key_when_flag_omitted(self) -> None:
        os.environ["ELASTIC_API_KEY"] = "env-api-key"
        os.environ["ELASTIC_URL"] = "https://example.es.io"

        config = build_config(
            Namespace(
                frozen_dir="/tmp/frozen",
                elastic_url=None,
                api_key=None,
                checkpoint=None,
                ingest_iteration=None,
                index_timestamp=None,
                skip_metadata=False,
                partition_strategy=None,
            )
        )

        self.assertEqual(config.api_key, "env-api-key")
        self.assertEqual(config.elastic_url, "https://example.es.io")

    def test_cli_api_key_overrides_env(self) -> None:
        os.environ["ELASTIC_API_KEY"] = "env-api-key"

        config = build_config(
            Namespace(
                frozen_dir="/tmp/frozen",
                elastic_url=None,
                api_key="cli-api-key",
                checkpoint=None,
                ingest_iteration=None,
                index_timestamp=None,
                skip_metadata=False,
                partition_strategy=None,
            )
        )

        self.assertEqual(config.api_key, "cli-api-key")

    def test_iteration_flag_is_applied(self) -> None:
        config = build_config(
            Namespace(
                frozen_dir="/tmp/frozen",
                elastic_url=None,
                api_key=None,
                checkpoint=None,
                ingest_iteration="1.2",
                index_timestamp="20250713120000",
                skip_metadata=False,
                partition_strategy=None,
            )
        )

        self.assertEqual(config.ingest_iteration, "1.2")
        self.assertEqual(config.index_timestamp, "20250713120000")


if __name__ == "__main__":
    unittest.main()
