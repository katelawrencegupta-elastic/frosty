"""Tests for Phase 4 bulk pipeline overlap."""

from __future__ import annotations

import gzip
import json
import os
import threading
import unittest
from unittest import mock

from frosty.elastic import (
    _bulk_api_path,
    bulk_flush_workers,
    bulk_gzip_level,
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

    def test_pipeline_enabled_by_default(self) -> None:
        os.environ.pop("FROSTY_BULK_PIPELINE_ENABLED", None)
        self.assertTrue(bulk_pipeline_enabled())

    def test_pipeline_disabled_from_env(self) -> None:
        os.environ["FROSTY_BULK_PIPELINE_ENABLED"] = "false"
        self.assertFalse(bulk_pipeline_enabled())

    def test_prefetch_defaults_to_four(self) -> None:
        os.environ.pop("FROSTY_BULK_PIPELINE_PREFETCH", None)
        self.assertEqual(bulk_pipeline_prefetch_batches(), 4)

    def test_gzip_level_defaults_to_one(self) -> None:
        os.environ.pop("FROSTY_BULK_GZIP_LEVEL", None)
        self.assertEqual(bulk_gzip_level(), 1)
        os.environ["FROSTY_BULK_GZIP_LEVEL"] = "9"
        self.assertEqual(bulk_gzip_level(), 9)
        os.environ["FROSTY_BULK_GZIP_LEVEL"] = "99"
        self.assertEqual(bulk_gzip_level(), 9)

    def test_flush_workers_defaults_to_two(self) -> None:
        os.environ.pop("FROSTY_BULK_FLUSH_WORKERS", None)
        self.assertEqual(bulk_flush_workers(), 2)
        os.environ["FROSTY_BULK_FLUSH_WORKERS"] = "3"
        self.assertEqual(bulk_flush_workers(), 3)

    def test_bulk_refresh_disabled_by_default(self) -> None:
        os.environ.pop("FROSTY_BULK_REFRESH", None)
        self.assertFalse(bulk_refresh_enabled())
        path = _bulk_api_path("frosty-apache-1.0-test")
        self.assertTrue(path.startswith("/frosty-apache-1.0-test/_bulk?refresh=false"))
        self.assertIn("filter_path=", path)

    def test_bulk_refresh_wait_for_when_enabled(self) -> None:
        os.environ["FROSTY_BULK_REFRESH"] = "true"
        self.assertTrue(bulk_refresh_enabled())
        path = _bulk_api_path("frosty-apache-1.0-test")
        self.assertTrue(path.startswith("/frosty-apache-1.0-test/_bulk?refresh=wait_for"))


class BulkPipelineBehaviorTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env = os.environ.copy()

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._env)

    def _docs(self, count: int):
        for index in range(count):
            yield {"@timestamp": "2025-01-01T00:00:00+00:00", "message": f"line-{index}"}

    def test_sync_and_pipelined_index_same_document_count(self) -> None:
        flush_calls: list[int] = []

        def _fake_flush(batch, *, elastic_url, api_key, index_name):
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
        os.environ["FROSTY_BULK_FLUSH_WORKERS"] = "1"
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
        self.assertTrue(any(name.startswith("frosty-bulk-consumer") for name in thread_names))

    def test_pipeline_spawns_configured_flush_workers(self) -> None:
        os.environ["FROSTY_BULK_FLUSH_WORKERS"] = "3"
        thread_names: list[str] = []
        original_thread = threading.Thread

        def _track_thread(*args, **kwargs):
            thread = original_thread(*args, **kwargs)
            thread_names.append(kwargs.get("name", ""))
            return thread

        with mock.patch("frosty.elastic._flush_bulk_batch", return_value=(1, 0)):
            with mock.patch("frosty.elastic.threading.Thread", side_effect=_track_thread):
                bulk_index(
                    "https://example.es.io",
                    "key",
                    "frosty-apache-1.0-test",
                    self._docs(3),
                    batch_size=1,
                    pipeline=True,
                )

        consumers = [name for name in thread_names if name.startswith("frosty-bulk-consumer")]
        self.assertEqual(len(consumers), 3)

    def test_flush_uses_configured_gzip_level(self) -> None:
        os.environ["FROSTY_BULK_GZIP_LEVEL"] = "1"
        captured: dict = {}

        def _fake_request(elastic_url, api_key, method, path, body=None, **kwargs):
            captured["body"] = body
            return 200, {"errors": False}

        with mock.patch("frosty.elastic.elastic_request", side_effect=_fake_request):
            with mock.patch("frosty.elastic.gzip.compress", side_effect=gzip.compress) as compress:
                from frosty.elastic import _flush_bulk_batch

                _flush_bulk_batch(
                    [b'{"index":{}}', b'{"message":"a"}'],
                    elastic_url="https://example.es.io",
                    api_key="key",
                    index_name="frosty-apache",
                )
                self.assertEqual(compress.call_args.kwargs.get("compresslevel"), 1)
                self.assertTrue(captured["body"])

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
