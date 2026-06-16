"""Convert frosty domain objects to JSON-serializable dicts."""

from __future__ import annotations

from frosty.buckets import FrozenBucket
from frosty.models import (
    BucketCount,
    BucketIngestResult,
    DryRunResult,
    IngestResult,
    PipelineSetupResult,
    ScanResult,
)


def bucket_to_dict(bucket: FrozenBucket) -> dict:
    return {
        "index_name": bucket.index_name,
        "bucket_name": bucket.bucket_name,
        "bucket_key": bucket.bucket_key,
        "elastic_index": bucket.elastic_index,
        "journal_path": str(bucket.journal_path),
        "latest_epoch": bucket.latest_epoch,
        "earliest_epoch": bucket.earliest_epoch,
    }


def scan_result_to_dict(result: ScanResult) -> dict:
    return {
        "bucket_count": len(result.buckets),
        "indices": [
            {
                "index_name": p.index_name,
                "event_kinds": [k.value for k in p.event_kinds],
                "router_pipeline": p.router_pipeline,
                "by_kind": {k.value: c for k, c in p.profile.by_kind.items()},
                "by_sourcetype": dict(p.profile.by_sourcetype.most_common(50)),
            }
            for p in result.indices
        ],
    }


def dry_run_to_dict(result: DryRunResult) -> dict:
    return {
        "total_events": result.total_events,
        "by_kind": {k.value: c for k, c in result.profile.by_kind.items()},
        "buckets": [
            {
                "bucket_key": item.bucket.bucket_key,
                "event_count": item.event_count,
            }
            for item in result.buckets
        ],
    }


def ingest_result_to_dict(result: IngestResult) -> dict:
    return {
        "total_indexed": result.total_indexed,
        "total_errors": result.total_errors,
        "skipped": result.skipped,
        "failed": result.failed,
        "buckets": [
            {
                "bucket_key": b.bucket_key,
                "elastic_index": b.elastic_index,
                "status": b.status,
                "indexed": b.indexed,
                "errors": b.errors,
            }
            for b in result.buckets
        ],
    }


def pipeline_setup_to_dict(result: PipelineSetupResult) -> dict:
    return {
        "deployed_pipelines": result.deployed_pipelines,
        "index_routers": result.index_routers,
        "reindex_results": result.reindex_results,
    }
