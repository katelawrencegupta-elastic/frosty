"""Tests for ingest concurrency and scale hardening."""

from __future__ import annotations

import os
import tempfile
import threading
import time
import unittest
from concurrent.futures import Future
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path
from unittest import mock

from frosty.buckets import FrozenBucket, discover_buckets
from frosty.checkpoint import CheckpointStore
from frosty.client import FrostyClient
from frosty.config import FrostyConfig
from frosty.elastic import (
    _batch_should_flush,
    _get_pool,
    _retry_sleep,
    bulk_index,
    bulk_max_bytes,
    close_elastic_pool,
)


def _make_bucket(index: str, name: str, seq: int = 0) -> FrozenBucket:
    return FrozenBucket(
        index_name=index,
        bucket_name=name,
        latest_epoch=1,
        earliest_epoch=1,
        sequence=seq,
        journal_path=Path(f"/tmp/{index}/{name}/rawdata/journal.zst"),
    )


class BulkByteCapTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env = os.environ.copy()

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._env)

    def test_batch_should_flush_on_docs_or_bytes(self) -> None:
        self.assertFalse(
            _batch_should_flush(2, 100, batch_size=5, max_bytes=1000)
        )
        self.assertTrue(
            _batch_should_flush(10, 100, batch_size=5, max_bytes=1000)
        )
        self.assertTrue(
            _batch_should_flush(2, 1000, batch_size=5, max_bytes=1000)
        )

    def test_bulk_max_bytes_default(self) -> None:
        os.environ.pop("FROSTY_BULK_MAX_BYTES", None)
        self.assertEqual(bulk_max_bytes(), 15 * 1024 * 1024)

    def test_byte_cap_triggers_extra_flushes(self) -> None:
        os.environ["FROSTY_BULK_MAX_BYTES"] = "80"
        os.environ["FROSTY_BULK_PIPELINE_ENABLED"] = "false"
        flush_docs: list[int] = []

        def _fake_flush(batch, **_kwargs):
            flush_docs.append(len(batch) // 2)
            return len(batch) // 2, 0

        docs = [
            {"@timestamp": "2025-01-01T00:00:00+00:00", "message": "x" * 40}
            for _ in range(4)
        ]
        with mock.patch("frosty.elastic._flush_bulk_batch", side_effect=_fake_flush):
            indexed, errors = bulk_index(
                "https://example.es.io",
                "key",
                "frosty-apache-test",
                docs,
                batch_size=100,
                pipeline=False,
            )

        self.assertEqual(indexed, 4)
        self.assertEqual(errors, 0)
        self.assertGreater(len(flush_docs), 1)


class RetryJitterTests(unittest.TestCase):
    def test_retry_sleep_uses_equal_jitter(self) -> None:
        with mock.patch("frosty.elastic.time.sleep") as sleep_mock:
            with mock.patch(
                "frosty.elastic.random.uniform", return_value=0.25
            ) as uniform:
                _retry_sleep(2)
        uniform.assert_called_once_with(0.0, 2.0)
        sleep_mock.assert_called_once_with(2.0 + 0.25)

    def test_retry_sleep_caps_base(self) -> None:
        with mock.patch("frosty.elastic.time.sleep") as sleep_mock:
            with mock.patch("frosty.elastic.random.uniform", return_value=0.0):
                _retry_sleep(20)
        # base capped at 30 → sleep 15 + 0
        sleep_mock.assert_called_once_with(15.0)


class ElasticPoolLockTests(unittest.TestCase):
    def tearDown(self) -> None:
        close_elastic_pool()

    def test_concurrent_get_pool_returns_same_instance(self) -> None:
        close_elastic_pool()
        barrier = threading.Barrier(8)
        ids: list[int] = []
        lock = threading.Lock()

        def worker() -> None:
            barrier.wait()
            pool = _get_pool()
            with lock:
                ids.append(id(pool))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        self.assertEqual(len(ids), 8)
        self.assertEqual(len(set(ids)), 1)


class PipelineDeadlockTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env = os.environ.copy()
        os.environ["FROSTY_BULK_FLUSH_WORKERS"] = "1"
        os.environ["FROSTY_BULK_PIPELINE_PREFETCH"] = "1"

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self._env)

    def test_consumer_failure_does_not_hang(self) -> None:
        def _fail_flush(*_args, **_kwargs):
            raise RuntimeError("flush failed")

        docs = [
            {"@timestamp": "2025-01-01T00:00:00+00:00", "message": f"line-{i}"}
            for i in range(20)
        ]
        started = time.perf_counter()
        with mock.patch("frosty.elastic._flush_bulk_batch", side_effect=_fail_flush):
            with self.assertRaises(RuntimeError):
                bulk_index(
                    "https://example.es.io",
                    "key",
                    "frosty-apache-test",
                    docs,
                    batch_size=1,
                    pipeline=True,
                )
        self.assertLess(time.perf_counter() - started, 10.0)


class DiscoverBucketsTests(unittest.TestCase):
    def test_discover_is_lazy_and_prunes_indices(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for index in ("apache", "nginx"):
                for seq in (0, 1):
                    bucket = root / index / f"db_10_1_{seq}"
                    journal = bucket / "rawdata" / "journal.zst"
                    journal.parent.mkdir(parents=True)
                    journal.write_bytes(b"not-a-real-journal")

            gen = discover_buckets(root, indices=["apache"])
            self.assertFalse(isinstance(gen, list))
            buckets = list(gen)
            self.assertEqual(len(buckets), 2)
            self.assertTrue(all(b.index_name == "apache" for b in buckets))

            named = list(
                discover_buckets(
                    root,
                    indices=["nginx"],
                    bucket_names=["db_10_1_1"],
                )
            )
            self.assertEqual(len(named), 1)
            self.assertEqual(named[0].bucket_name, "db_10_1_1")


class ProcessPoolWindowTests(unittest.TestCase):
    def test_sliding_window_limits_outstanding_submits(self) -> None:
        buckets = [_make_bucket("apache", f"db_1_1_{i}", i) for i in range(6)]
        client = FrostyClient(
            FrostyConfig(
                frozen_dir=Path("/tmp"),
                api_key="k",
                elastic_url="https://example.es.io",
                container_workers=False,
            )
        )
        max_outstanding = [0]
        outstanding = [0]
        lock = threading.Lock()

        class TrackingFuture(Future):
            pass

        class TrackingPool:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def submit(self, fn, bucket, *args):
                fut = TrackingFuture()
                with lock:
                    outstanding[0] += 1
                    max_outstanding[0] = max(max_outstanding[0], outstanding[0])

                def _complete():
                    time.sleep(0.01)
                    with lock:
                        outstanding[0] -= 1
                    fut.set_result((1, 0, None))

                threading.Thread(target=_complete, daemon=True).start()
                return fut

        with mock.patch("frosty.client.ProcessPoolExecutor", TrackingPool):
            with mock.patch("frosty.client.record_ingest_bucket"):
                with mock.patch("frosty.client._record_parent_journal_decode"):
                    results = client._ingest_process_parallel(
                        buckets,
                        "https://example.es.io",
                        "k",
                        100,
                        workers=2,
                        checkpoint=None,
                        run_iteration="5.4",
                        index_timestamp="20260714120000",
                    )

        self.assertEqual(len(results), 6)
        self.assertLessEqual(max_outstanding[0], 2)

    def test_broken_process_pool_short_circuits(self) -> None:
        buckets = [_make_bucket("apache", f"db_1_1_{i}", i) for i in range(4)]
        client = FrostyClient(
            FrostyConfig(
                frozen_dir=Path("/tmp"),
                api_key="k",
                elastic_url="https://example.es.io",
                container_workers=False,
            )
        )
        submit_count = [0]

        class BrokenPool:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def submit(self, fn, bucket, *args):
                submit_count[0] += 1
                fut = Future()
                if submit_count[0] == 1:
                    fut.set_exception(BrokenProcessPool("boom"))
                else:
                    fut.set_result((1, 0, None))
                return fut

        with tempfile.TemporaryDirectory() as tmp:
            checkpoint = CheckpointStore(Path(tmp) / "c.db")
            try:
                with mock.patch("frosty.client.ProcessPoolExecutor", BrokenPool):
                    with mock.patch("frosty.client.record_ingest_bucket"):
                        with self.assertRaises(BrokenProcessPool):
                            client._ingest_process_parallel(
                                buckets,
                                "https://example.es.io",
                                "k",
                                100,
                                workers=2,
                                checkpoint=checkpoint,
                                run_iteration="5.4",
                                index_timestamp="20260714120000",
                            )
                # Window starts with 2 submits; broken pool must not drain all 4 as normal work.
                self.assertLessEqual(submit_count[0], 2)
            finally:
                checkpoint.close()

    def test_mark_in_progress_only_on_dispatch(self) -> None:
        buckets = [_make_bucket("apache", f"db_1_1_{i}", i) for i in range(5)]
        client = FrostyClient(
            FrostyConfig(
                frozen_dir=Path("/tmp"),
                api_key="k",
                elastic_url="https://example.es.io",
                container_workers=False,
            )
        )
        marks: list[str] = []

        class SlowPool:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def submit(self, fn, bucket, *args):
                fut = Future()
                fut.set_result((1, 0, None))
                return fut

        checkpoint = mock.Mock()
        checkpoint.mark_in_progress.side_effect = (
            lambda key, *_a: marks.append(key)
        )

        with mock.patch("frosty.client.ProcessPoolExecutor", SlowPool):
            with mock.patch("frosty.client.record_ingest_bucket"):
                with mock.patch("frosty.client._record_parent_journal_decode"):
                    # Intercept wait to observe marks before draining.
                    original_wait = __import__(
                        "frosty.client", fromlist=["wait"]
                    ).wait

                    def counting_wait(fs, return_when=None):
                        # After initial window of 2, only 2 marks should exist.
                        if len(marks) == 2 and len(list(fs)) == 2:
                            self.assertEqual(len(marks), 2)
                        return original_wait(fs, return_when=return_when)

                    with mock.patch("frosty.client.wait", side_effect=counting_wait):
                        client._ingest_process_parallel(
                            buckets,
                            "https://example.es.io",
                            "k",
                            100,
                            workers=2,
                            checkpoint=checkpoint,
                            run_iteration="5.4",
                            index_timestamp="20260714120000",
                        )

        self.assertEqual(checkpoint.mark_in_progress.call_count, 5)


if __name__ == "__main__":
    unittest.main()
