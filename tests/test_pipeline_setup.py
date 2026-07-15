"""Tests for automatic ingest pipeline setup (Level A)."""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

from frosty.buckets import FrozenBucket
from frosty.client import FrostyClient
from frosty.config import FrostyConfig
from frosty.event_types import EventKind
from frosty.models import PipelineSetupResult


def _bucket(index_name: str = "mixed", name: str = "db_1_1_0") -> FrozenBucket:
    return FrozenBucket(
        index_name=index_name,
        bucket_name=name,
        latest_epoch=1,
        earliest_epoch=1,
        sequence=0,
        journal_path=Path("/tmp/journal.zst"),
        index_iteration="9.2",
        index_timestamp="20260715120000",
    )


class SetupPipelinesTests(unittest.TestCase):
    def test_only_missing_reuses_existing_pipelines(self) -> None:
        config = FrostyConfig(
            frozen_dir=Path("/tmp"),
            elastic_url="https://es.example",
            api_key="key",
        )
        client = FrostyClient(config)
        put_calls: list[str] = []

        with (
            mock.patch.object(client, "verify_elastic", return_value={}),
            mock.patch("frosty.client.verify_cluster"),
            mock.patch(
                "frosty.client.list_pipelines",
                return_value={
                    "frosty-parse-access-log": {},
                    "frosty-parse-syslog": {},
                    "frosty-parse-generic": {},
                    "frosty-pipeline-mixed": {},
                },
            ),
            mock.patch(
                "frosty.client.put_pipeline",
                side_effect=lambda *a, **k: put_calls.append(a[2]),
            ),
            mock.patch("frosty.client.set_default_pipeline") as set_default,
        ):
            result = client.setup_pipelines(
                indices=["mixed"],
                event_kinds=list(EventKind),
                only_missing=True,
                elastic_indices={
                    "mixed": "frosty-mixed-9.2-20260715120000",
                },
            )

        self.assertEqual(put_calls, [])
        self.assertEqual(result.deployed_pipelines, [])
        self.assertIn("frosty-pipeline-mixed", result.reused_pipelines)
        set_default.assert_called_once_with(
            "https://es.example",
            "key",
            "frosty-mixed-9.2-20260715120000",
            "frosty-pipeline-mixed",
        )
        self.assertEqual(
            result.index_defaults["frosty-mixed-9.2-20260715120000"],
            "frosty-pipeline-mixed",
        )

    def test_creates_missing_pipelines_on_versioned_index(self) -> None:
        config = FrostyConfig(
            frozen_dir=Path("/tmp"),
            elastic_url="https://es.example",
            api_key="key",
        )
        client = FrostyClient(config)
        put_calls: list[str] = []

        with (
            mock.patch.object(client, "verify_elastic", return_value={}),
            mock.patch("frosty.client.verify_cluster"),
            mock.patch("frosty.client.list_pipelines", return_value={}),
            mock.patch(
                "frosty.client.put_pipeline",
                side_effect=lambda *a, **k: put_calls.append(a[2]),
            ),
            mock.patch("frosty.client.set_default_pipeline") as set_default,
        ):
            result = client.setup_pipelines(
                indices=["mixed"],
                event_kinds=[EventKind.GENERIC, EventKind.ACCESS_LOG],
                only_missing=True,
                elastic_indices={"mixed": "frosty-mixed-9.2-20260715120000"},
            )

        self.assertIn("frosty-parse-generic", result.deployed_pipelines)
        self.assertIn("frosty-parse-access-log", result.deployed_pipelines)
        self.assertIn("frosty-pipeline-mixed", result.deployed_pipelines)
        self.assertEqual(result.reused_pipelines, [])
        set_default.assert_called_once()

    def test_ingest_calls_setup_pipelines_before_workers(self) -> None:
        config = FrostyConfig(
            frozen_dir=Path("/tmp"),
            elastic_url="https://es.example",
            api_key="key",
            ingest_iteration="9.2",
            index_timestamp="20260715120000",
            container_workers=False,
            ingest_workers=1,
        )
        client = FrostyClient(config)
        bucket = _bucket()
        order: list[str] = []

        def _setup(**kwargs):
            order.append("setup")
            self.assertTrue(kwargs.get("only_missing"))
            self.assertEqual(
                kwargs.get("elastic_indices"),
                {"mixed": bucket.elastic_index},
            )
            return PipelineSetupResult(
                deployed_pipelines=["frosty-pipeline-mixed"],
                index_defaults={bucket.elastic_index: "frosty-pipeline-mixed"},
            )

        with (
            mock.patch.object(client, "list_buckets", return_value=[bucket]),
            mock.patch.object(client, "verify_elastic", return_value={"version": {"number": "9"}}),
            mock.patch("frosty.client.verify_cluster"),
            mock.patch("frosty.client.ensure_index", side_effect=lambda *a: order.append("ensure")),
            mock.patch.object(client, "setup_pipelines", side_effect=_setup),
            mock.patch("frosty.client.ensure_apm_client"),
            mock.patch("frosty.client.begin_transaction") as begin_tx,
            mock.patch.object(
                client,
                "_ingest_one_bucket",
                side_effect=lambda *a, **k: (_ for _ in ()).throw(
                    AssertionError("stop after setup")
                ),
            ),
        ):
            begin_tx.return_value.__enter__ = mock.Mock(return_value=None)
            begin_tx.return_value.__exit__ = mock.Mock(return_value=False)
            with self.assertRaises(AssertionError):
                client.ingest(force=True, workers=1)

        self.assertEqual(order, ["ensure", "setup"])

    def test_ingest_can_skip_pipeline_setup(self) -> None:
        config = FrostyConfig(
            frozen_dir=Path("/tmp"),
            elastic_url="https://es.example",
            api_key="key",
            ingest_iteration="9.2",
            index_timestamp="20260715120000",
            container_workers=False,
            ingest_workers=1,
        )
        client = FrostyClient(config)
        bucket = _bucket()

        with (
            mock.patch.object(client, "list_buckets", return_value=[bucket]),
            mock.patch.object(client, "verify_elastic", return_value={"version": {"number": "9"}}),
            mock.patch("frosty.client.verify_cluster"),
            mock.patch("frosty.client.ensure_index"),
            mock.patch.object(client, "setup_pipelines") as setup,
            mock.patch("frosty.client.ensure_apm_client"),
            mock.patch("frosty.client.begin_transaction") as begin_tx,
            mock.patch.object(
                client,
                "_ingest_one_bucket",
                side_effect=lambda *a, **k: (_ for _ in ()).throw(
                    AssertionError("stop")
                ),
            ),
        ):
            begin_tx.return_value.__enter__ = mock.Mock(return_value=None)
            begin_tx.return_value.__exit__ = mock.Mock(return_value=False)
            with self.assertRaises(AssertionError):
                client.ingest(force=True, workers=1, setup_pipelines=False)
        setup.assert_not_called()


if __name__ == "__main__":
    unittest.main()
