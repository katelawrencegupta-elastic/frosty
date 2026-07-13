"""Tests for Phase 2 performance and metrics aggregation."""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

from frosty.buckets import FrozenBucket
from frosty.ingest_ops import ingest_bucket
from frosty.journal import decode_message
from frosty.metrics import (
    JOURNAL_DECODES_TOTAL,
    JournalDecodeMetrics,
    journal_decode_metrics_from_dict,
    record_journal_decode,
)
from frosty.text_decode import decode_message_bytes, uses_latin1_decode


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


class TextDecodeFastPathTests(unittest.TestCase):
    def test_apache_index_uses_latin1(self) -> None:
        self.assertTrue(uses_latin1_decode(splunk_index="apache"))

    def test_decode_message_bytes_latin1_for_apache(self) -> None:
        raw = b"\xe9"  # latin-1 only byte
        decoded = decode_message_bytes(raw, splunk_index="apache")
        self.assertEqual(decoded, "\xe9")

    def test_decode_message_wrapper_delegates(self) -> None:
        raw = b"hello"
        self.assertEqual(decode_message(raw), "hello")


class JournalMetricsAggregationTests(unittest.TestCase):
    def test_ingest_bucket_returns_decode_metrics_without_prometheus(self) -> None:
        bucket = _sample_bucket()
        decode_metrics = JournalDecodeMetrics(
            journal_path=str(bucket.journal_path),
            journal_size_bytes=2048,
            decode_duration_ms=25.0,
            event_count=3,
        )

        def _fake_iter_bucket_docs(*_args, **kwargs):
            holder = kwargs.get("metrics_holder")
            if holder is not None:
                holder.metrics = decode_metrics
            yield {"@timestamp": "2025-01-01T00:00:00+00:00", "message": "ok"}

        def _fake_bulk_index(_elastic_url, _api_key, _index_name, docs, *, batch_size=500):
            list(docs)
            return 1, 0

        with mock.patch("frosty.ingest_ops.bulk_index", side_effect=_fake_bulk_index):
            with mock.patch("frosty.journal.iter_bucket_docs", side_effect=_fake_iter_bucket_docs):
                indexed, errors, status, returned_metrics = ingest_bucket(
                bucket,
                "https://example.es.io",
                "test-key",
                500,
                prometheus=False,
                )

        self.assertEqual(status, "completed")
        self.assertEqual(indexed, 1)
        self.assertEqual(errors, 0)
        self.assertIsNotNone(returned_metrics)
        assert returned_metrics is not None
        self.assertEqual(returned_metrics.event_count, 3)

    def test_parent_records_journal_metrics_from_worker_payload(self) -> None:
        metrics = JournalDecodeMetrics(
            journal_path="/data/frozen/apache/db_1/rawdata/journal.zst",
            journal_size_bytes=2048,
            decode_duration_ms=50.0,
            event_count=10,
        )
        restored = journal_decode_metrics_from_dict(metrics.to_dict())
        self.assertIsNotNone(restored)
        assert restored is not None
        self.assertEqual(restored.event_count, 10)

        before = JOURNAL_DECODES_TOTAL.labels(
            index_name="apache",
            ingest_iteration="1.0",
            index_timestamp="20250713120000",
        )._value.get()

        record_journal_decode(
            restored,
            bucket_key="apache/db_1",
            index_name="apache",
            ingest_iteration="1.0",
            index_timestamp="20250713120000",
            prometheus=True,
            log=False,
        )

        after = JOURNAL_DECODES_TOTAL.labels(
            index_name="apache",
            ingest_iteration="1.0",
            index_timestamp="20250713120000",
        )._value.get()
        self.assertEqual(after, before + 1)


class ElasticPoolTests(unittest.TestCase):
    def test_elastic_request_uses_shared_pool(self) -> None:
        from frosty import elastic

        elastic.close_elastic_pool()
        response = mock.Mock()
        response.status = 200
        response.data = b'{"ok": true}'

        with mock.patch.object(elastic, "_get_pool") as get_pool:
            pool = mock.Mock()
            pool.request.return_value = response
            get_pool.return_value = pool

            status, payload = elastic.elastic_request(
                "https://example.es.io",
                "test-key",
                "GET",
                "/",
            )

        self.assertEqual(status, 200)
        self.assertEqual(payload, {"ok": True})
        pool.request.assert_called_once()
        elastic.close_elastic_pool()


if __name__ == "__main__":
    unittest.main()
