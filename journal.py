"""Decode Splunk journal.zst files into Elasticsearch-ready documents."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import zstandard as zstd

from frosty.buckets import FrozenBucket
from frosty.event_types import ClassifiedEvent, classify_event, strip_splunk_prefix
from frosty.splunk_journal.decoder import JournalDecoder


def decode_message(raw: bytes) -> str:
    """Decode event bytes as UTF-8, falling back to Latin-1."""
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin-1")


def event_time_to_iso(event_time_ms: int) -> str:
    """Convert Splunk event time (milliseconds) to ISO-8601 UTC."""
    seconds = event_time_ms / 1000.0
    return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat()


def classify_journal_event(event: dict, bucket: FrozenBucket) -> ClassifiedEvent:
    """Classify a decoded journal event and determine its ingest pipeline."""
    message = event.get("event", "")
    if isinstance(message, bytes):
        message = decode_message(message)

    return classify_event(
        sourcetype=str(event.get("sourcetype", "")),
        source=str(event.get("source", "")),
        message=message,
        splunk_index=bucket.index_name,
    )


def to_elastic_doc(event: dict, bucket: FrozenBucket) -> dict:
    """Map a decoded journal event to an Elasticsearch document."""
    message = event.get("event", "")
    if isinstance(message, bytes):
        message = decode_message(message)

    classified = classify_journal_event(event, bucket)

    doc = {
        "@timestamp": event_time_to_iso(int(event.get("time", 0))),
        "message": message,
        "host": strip_splunk_prefix(str(event.get("host", ""))),
        "source": strip_splunk_prefix(str(event.get("source", ""))),
        "sourcetype": strip_splunk_prefix(str(event.get("sourcetype", ""))),
        "event": {
            "kind": classified.kind.value,
            "dataset": classified.dataset,
        },
        "splunk.index": bucket.index_name,
        "splunk.index_time": int(event.get("index_time", 0)),
        "splunk.bucket_name": bucket.bucket_name,
        "splunk.bucket_latest": bucket.latest_epoch,
        "splunk.bucket_earliest": bucket.earliest_epoch,
        "splunk.pipeline": classified.pipeline_name,
        "splunk.classify_reason": classified.reason,
    }

    fields = event.get("fields") or {}
    if fields:
        doc["splunk.fields"] = fields

    return doc


def iter_journal_events(journal_path: Path) -> Iterator[dict]:
    """Yield decoded events from a journal.zst file."""
    with journal_path.open("rb") as handle:
        decompressor = zstd.ZstdDecompressor()
        with decompressor.stream_reader(handle) as reader:
            decoder = JournalDecoder(reader=reader)
            while decoder.scan():
                yield decoder.get_event().to_normalized_dict()
            err = decoder.err()
            if err is not None:
                raise RuntimeError(f"Journal decode error in {journal_path}: {err}") from err


def iter_bucket_docs(bucket: FrozenBucket) -> Iterator[dict]:
    """Yield Elasticsearch documents for all events in a bucket."""
    for event in iter_journal_events(bucket.journal_path):
        yield to_elastic_doc(event, bucket)
