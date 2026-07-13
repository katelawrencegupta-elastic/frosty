"""Tests for decode vs bulk phase metrics."""

from __future__ import annotations

import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

from frosty.buckets import FrozenBucket
from frosty.ingest_ops import ingest_bucket
from frosty.metrics import (
    BUCKET_BULK_DURATION_SECONDS,
    JOURNAL_DECODE_DURATION_SECONDS,
    JOURNAL_READ_DURATION_SECONDS,
    JournalDecodeMetrics,
    MetricsHolder,
)
from frosty.splunk_journal.stream import JournalStream


def _sample_bucket() -> FrozenBucket:
    return FrozenBucket(
        index_name="apache",
        bucket_name="db_1_0_0",
        latest_epoch=1,
        earliest_epoch=0,
        sequence=0,
        journal_path=Path("/tmp/journal.zst"),
        index_iteration="1.0",
        index_timestamp="20250713120000",
    )


class BucketPhaseMetricsTests(unittest.TestCase):
    def test_ingest_bucket_records_decode_and_bulk_separately(self) -> None:
        bucket = _sample_bucket()
        decode_metrics = JournalDecodeMetrics(
            journal_path=str(bucket.journal_path),
            journal_size_bytes=2048,
            decode_duration_ms=12.0,
            event_count=2,
        )

        def _fake_iter_bucket_docs(*_args, **kwargs):
            holder: MetricsHolder | None = kwargs.get("metrics_holder")
            if holder is not None:
                holder.metrics = decode_metrics
            yield {"@timestamp": "2025-01-01T00:00:00+00:00", "message": "one"}
            time.sleep(0.05)
            yield {"@timestamp": "2025-01-01T00:00:01+00:00", "message": "two"}

        def _slow_bulk(
            _elastic_url,
            _api_key,
            _index_name,
            docs,
            *,
            batch_size=500,
            metrics_holder=None,
        ):
            list(docs)
            time.sleep(0.04)
            if metrics_holder is not None and metrics_holder.metrics is not None:
                metrics_holder.metrics = replace(
                    metrics_holder.metrics,
                    bulk_duration_ms=40.0,
                )
            return 2, 0

        labels = {
            "index_name": "apache",
            "ingest_iteration": "1.0",
            "index_timestamp": "20250713120000",
        }
        decode_before = JOURNAL_DECODE_DURATION_SECONDS.labels(**labels)._sum.get()
        bulk_before = BUCKET_BULK_DURATION_SECONDS.labels(**labels)._sum.get()

        with mock.patch("frosty.ingest_ops.bulk_index", side_effect=_slow_bulk):
            with mock.patch("frosty.journal.iter_bucket_docs", side_effect=_fake_iter_bucket_docs):
                indexed, errors, status, returned_metrics = ingest_bucket(
                    bucket,
                    "https://example.es.io",
                    "test-key",
                    500,
                    prometheus=True,
                )

        self.assertEqual(status, "completed")
        self.assertEqual(indexed, 2)
        self.assertEqual(errors, 0)
        assert returned_metrics is not None
        self.assertEqual(returned_metrics.decode_duration_ms, 12.0)
        self.assertEqual(returned_metrics.bulk_duration_ms, 40.0)

        decode_after = JOURNAL_DECODE_DURATION_SECONDS.labels(**labels)._sum.get()
        bulk_after = BUCKET_BULK_DURATION_SECONDS.labels(**labels)._sum.get()
        self.assertAlmostEqual(decode_after - decode_before, 0.012, places=3)
        self.assertAlmostEqual(bulk_after - bulk_before, 0.04, places=3)

    def test_journal_stream_accumulates_read_duration(self) -> None:
        class SlowReader:
            def read(self, size: int) -> bytes:
                time.sleep(0.02)
                return b"\x00" * size

        read_duration_ms = [0.0]
        stream = JournalStream(SlowReader(), read_duration_ms=read_duration_ms)
        stream.read(1024)
        self.assertGreaterEqual(read_duration_ms[0], 15.0)

    def test_ingest_bucket_records_read_duration(self) -> None:
        bucket = _sample_bucket()
        decode_metrics = JournalDecodeMetrics(
            journal_path=str(bucket.journal_path),
            journal_size_bytes=2048,
            read_duration_ms=30.0,
            decode_duration_ms=12.0,
            event_count=1,
        )

        def _fake_iter_bucket_docs(*_args, **kwargs):
            holder: MetricsHolder | None = kwargs.get("metrics_holder")
            if holder is not None:
                holder.metrics = decode_metrics
            yield {"@timestamp": "2025-01-01T00:00:00+00:00", "message": "one"}

        def _fake_bulk(_elastic_url, _api_key, _index_name, docs, *, batch_size=500, metrics_holder=None):
            list(docs)
            return 1, 0

        labels = {
            "index_name": "apache",
            "ingest_iteration": "1.0",
            "index_timestamp": "20250713120000",
        }
        read_before = JOURNAL_READ_DURATION_SECONDS.labels(**labels)._sum.get()

        with mock.patch("frosty.ingest_ops.bulk_index", side_effect=_fake_bulk):
            with mock.patch("frosty.journal.iter_bucket_docs", side_effect=_fake_iter_bucket_docs):
                _indexed, _errors, status, returned_metrics = ingest_bucket(
                    bucket,
                    "https://example.es.io",
                    "test-key",
                    500,
                    prometheus=True,
                )

        self.assertEqual(status, "completed")
        assert returned_metrics is not None
        self.assertEqual(returned_metrics.read_duration_ms, 30.0)
        read_after = JOURNAL_READ_DURATION_SECONDS.labels(**labels)._sum.get()
        self.assertAlmostEqual(read_after - read_before, 0.03, places=3)


if __name__ == "__main__":
    unittest.main()
