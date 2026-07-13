"""Tests for Phase 4 bulk pipeline overlap."""

from __future__ import annotations

import json
import os
import threading
import unittest
from unittest import mock

from frosty.elastic import (
    _bulk_api_path,
    bulk_index,
    bulk_pipeline_enabled,
    bulk_pipeline_prefetch_batches,
    bulk_refresh_enabled,
)


class BulkPipelineConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env = os.environ.copy()

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._env)

    def test_pipeline_disabled_by_default(self) -> None:
        os.environ.pop("FROSTY_BULK_PIPELINE_ENABLED", None)
        self.assertFalse(bulk_pipeline_enabled())

    def test_pipeline_enabled_from_env(self) -> None:
        os.environ["FROSTY_BULK_PIPELINE_ENABLED"] = "true"
        self.assertTrue(bulk_pipeline_enabled())

    def test_prefetch_defaults_to_one(self) -> None:
        os.environ.pop("FROSTY_BULK_PIPELINE_PREFETCH", None)
        self.assertEqual(bulk_pipeline_prefetch_batches(), 1)

    def test_bulk_refresh_disabled_by_default(self) -> None:
        os.environ.pop("FROSTY_BULK_REFRESH", None)
        self.assertFalse(bulk_refresh_enabled())
        self.assertEqual(_bulk_api_path(), "/_bulk?refresh=false")

    def test_bulk_refresh_wait_for_when_enabled(self) -> None:
        os.environ["FROSTY_BULK_REFRESH"] = "true"
        self.assertTrue(bulk_refresh_enabled())
        self.assertEqual(_bulk_api_path(), "/_bulk?refresh=wait_for")


class BulkPipelineBehaviorTests(unittest.TestCase):
    def _docs(self, count: int):
        for index in range(count):
            yield {"@timestamp": "2025-01-01T00:00:00+00:00", "message": f"line-{index}"}

    def test_sync_and_pipelined_index_same_document_count(self) -> None:
        flush_calls: list[int] = []

        def _fake_flush(batch, *, elastic_url, api_key):
            flush_calls.append(len(batch) // 2)
            return len(batch) // 2, 0

        with mock.patch("frosty.elastic._flush_bulk_batch", side_effect=_fake_flush):
            sync_indexed, sync_errors = bulk_index(
                "https://example.es.io",
                "key",
                "frosty-apache-1.0-test",
                self._docs(5),
                batch_size=2,
                pipeline=False,
            )
            pipelined_indexed, pipelined_errors = bulk_index(
                "https://example.es.io",
                "key",
                "frosty-apache-1.0-test",
                self._docs(5),
                batch_size=2,
                pipeline=True,
            )

        self.assertEqual(sync_indexed, 5)
        self.assertEqual(sync_errors, 0)
        self.assertEqual(pipelined_indexed, 5)
        self.assertEqual(pipelined_errors, 0)
        self.assertEqual(sum(flush_calls), 10)

    def test_pipeline_uses_separate_threads(self) -> None:
        thread_names: list[str] = []
        original_thread = threading.Thread

        def _track_thread(*args, **kwargs):
            thread = original_thread(*args, **kwargs)
            thread_names.append(kwargs.get("name", ""))
            return thread

        with mock.patch("frosty.elastic._flush_bulk_batch", return_value=(2, 0)):
            with mock.patch("frosty.elastic.threading.Thread", side_effect=_track_thread):
                bulk_index(
                    "https://example.es.io",
                    "key",
                    "frosty-apache-1.0-test",
                    self._docs(4),
                    batch_size=2,
                    pipeline=True,
                )

        self.assertIn("frosty-bulk-producer", thread_names)
        self.assertIn("frosty-bulk-consumer", thread_names)

    def test_producer_error_propagates(self) -> None:
        json_line = json.dumps({"index": {"_index": "frosty-apache"}})

        def _boom(_index_name, _docs):
            yield json_line
            raise RuntimeError("decode failed")

        with mock.patch("frosty.elastic.bulk_lines", side_effect=_boom):
            with self.assertRaises(RuntimeError):
                bulk_index(
                    "https://example.es.io",
                    "key",
                    "frosty-apache-1.0-test",
                    self._docs(1),
                    pipeline=True,
                )


if __name__ == "__main__":
    unittest.main()
