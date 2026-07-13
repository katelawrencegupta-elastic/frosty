"""Tests for iteration 1.6 review follow-ups (bulk id, metrics, serializers)."""

from __future__ import annotations

import gzip
import json
import logging
import unittest
from unittest import mock

from frosty.api.serializers import ingest_result_to_dict
from frosty.elastic import (
    BULK_ID_FIELD,
    bulk_lines,
    document_bulk_id,
    _flush_bulk_batch,
)
from frosty.metrics import (
    INGEST_DURATION_SECONDS,
    JournalDecodeMetrics,
    _prom_run_labels,
    record_ingest_run,
)
from frosty.models import BucketIngestResult, IngestResult


class DocumentBulkIdTests(unittest.TestCase):
    def test_document_bulk_id_is_stable(self) -> None:
        left = document_bulk_id("apache/db_1", 1, 2, 3)
        right = document_bulk_id("apache/db_1", 1, 2, 3)
        self.assertEqual(left, right)
        self.assertNotEqual(left, document_bulk_id("apache/db_1", 1, 2, 4))

    def test_bulk_lines_emit_id_without_index_and_strip_private_field(self) -> None:
        doc = {
            "@timestamp": "2025-01-01T00:00:00+00:00",
            "message": "hello",
            BULK_ID_FIELD: "abc123",
        }
        lines = list(bulk_lines("frosty-apache-1.6-test", [doc]))
        self.assertEqual(len(lines), 2)
        action = json.loads(lines[0].decode())
        body = json.loads(lines[1].decode())
        self.assertEqual(action, {"index": {"_id": "abc123"}})
        self.assertNotIn(BULK_ID_FIELD, body)
        self.assertEqual(body["message"], "hello")


class BulkFlushTests(unittest.TestCase):
    def test_flush_gzips_body_and_logs_item_errors(self) -> None:
        captured: dict = {}

        def _fake_request(
            elastic_url,
            api_key,
            method,
            path,
            body=None,
            *,
            content_type="application/json",
            content_encoding=None,
            timeout=120,
            retries=5,
        ):
            captured["path"] = path
            captured["content_encoding"] = content_encoding
            captured["body"] = body
            return 200, {
                "errors": True,
                "items": [
                    {"index": {"error": {"type": "mapper_parsing_exception", "reason": "bad"}}},
                    {"index": {"status": 201}},
                ],
            }

        batch = list(
            bulk_lines(
                "frosty-apache",
                [
                    {"message": "a", BULK_ID_FIELD: "1"},
                    {"message": "b", BULK_ID_FIELD: "2"},
                ],
            )
        )
        with mock.patch("frosty.elastic.elastic_request", side_effect=_fake_request):
            with self.assertLogs("frosty.elastic", level=logging.WARNING) as logs:
                indexed, errors = _flush_bulk_batch(
                    batch,
                    elastic_url="https://example.es.io",
                    api_key="key",
                    index_name="frosty-apache",
                )

        self.assertEqual((indexed, errors), (1, 1))
        self.assertEqual(captured["content_encoding"], "gzip")
        self.assertTrue(captured["path"].startswith("/frosty-apache/_bulk?"))
        plain = gzip.decompress(captured["body"])
        self.assertTrue(plain.endswith(b"\n"))
        self.assertTrue(any("mapper_parsing_exception" in line for line in logs.output))


class MetricsLabelTests(unittest.TestCase):
    def test_prom_run_labels_exclude_per_run_cardinality(self) -> None:
        labels = _prom_run_labels(
            "apache",
            ingest_iteration="2.0",
            index_timestamp="20260713120000",
        )
        self.assertEqual(labels["ingest_iteration"], "2.0")
        self.assertEqual(labels["index_name"], "apache")
        self.assertNotIn("ingest_run", labels)
        self.assertNotIn("index_timestamp", labels)


class SerializerDecodeMetricsTests(unittest.TestCase):
    def test_ingest_result_includes_decode_metrics(self) -> None:
        metrics = JournalDecodeMetrics(
            journal_path="/tmp/j.zst",
            journal_size_bytes=10,
            decode_duration_ms=1.5,
            event_count=3,
            process_cpu_seconds=0.2,
            process_duration_seconds=1.0,
        )
        result = IngestResult(
            ingest_iteration="1.6",
            index_timestamp="20260713120000",
            duration_seconds=12.5,
            buckets=[
                BucketIngestResult(
                    bucket_key="apache/db_1",
                    index_name="apache",
                    bucket_name="db_1",
                    elastic_index="frosty-apache-1.6-20260713120000",
                    indexed=3,
                    errors=0,
                    status="completed",
                    decode_metrics=metrics,
                )
            ],
        )
        payload = ingest_result_to_dict(result)
        self.assertEqual(payload["ingest_run"], "1.6-20260713120000")
        self.assertEqual(payload["duration_seconds"], 12.5)
        self.assertEqual(payload["buckets"][0]["decode_metrics"]["event_count"], 3)
        self.assertEqual(payload["buckets"][0]["decode_metrics"]["process_cpu_seconds"], 0.2)


class IngestWallMetricTests(unittest.TestCase):
    def test_record_ingest_run_sets_gauge(self) -> None:
        labels = {"ingest_iteration": "2.0"}
        record_ingest_run(
            duration_seconds=192.589,
            ingest_iteration="2.0",
            index_timestamp="20260713205051",
            log=False,
        )
        self.assertEqual(INGEST_DURATION_SECONDS.labels(**labels)._value.get(), 192.589)


if __name__ == "__main__":
    unittest.main()
