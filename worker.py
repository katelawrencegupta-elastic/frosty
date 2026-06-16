"""Parallel bucket ingest worker (module-level for ProcessPoolExecutor)."""

from __future__ import annotations

from frosty.buckets import FrozenBucket
from frosty.elastic import bulk_index
from frosty.journal import iter_bucket_docs


def ingest_bucket_task(
    bucket: FrozenBucket,
    elastic_url: str,
    api_key: str,
    batch_size: int,
) -> tuple[int, int]:
    """Decode and bulk-index a single bucket. Returns (indexed, errors)."""
    return bulk_index(
        elastic_url,
        api_key,
        bucket.elastic_index,
        iter_bucket_docs(bucket),
        batch_size=batch_size,
    )
