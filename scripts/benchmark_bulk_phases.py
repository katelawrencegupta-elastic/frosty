#!/usr/bin/env python3
"""Compare read/decode/bulk phase timings across batch sizes and pipeline modes."""

from __future__ import annotations

import argparse
import os
import sys
import time

from frosty.buckets import discover_buckets, with_run_context
from frosty.config import FrostyConfig
from frosty.ingest_ops import ingest_bucket


def _run_case(
    bucket,
    *,
    elastic_url: str,
    api_key: str,
    batch_size: int,
    pipeline: bool,
) -> dict:
    os.environ["FROSTY_BULK_PIPELINE_ENABLED"] = "true" if pipeline else "false"
    wall_start = time.perf_counter()
    indexed, errors, status, metrics = ingest_bucket(
        bucket,
        elastic_url,
        api_key,
        batch_size,
        prometheus=False,
    )
    wall_ms = (time.perf_counter() - wall_start) * 1000.0
    if metrics is None:
        raise RuntimeError("missing decode metrics")
    bulk_s = metrics.bulk_duration_ms / 1000.0
    docs_per_bulk_s = indexed / bulk_s if bulk_s > 0 else 0.0
    return {
        "status": status,
        "indexed": indexed,
        "errors": errors,
        "events": metrics.event_count,
        "read_ms": metrics.read_duration_ms,
        "decode_ms": metrics.decode_duration_ms,
        "bulk_ms": metrics.bulk_duration_ms,
        "wall_ms": wall_ms,
        "docs_per_bulk_s": docs_per_bulk_s,
        "bulk_pct": (metrics.bulk_duration_ms / wall_ms * 100.0) if wall_ms > 0 else 0.0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--frozen-dir",
        default=os.environ.get("FROSTY_FROZEN_DIR", "/Users/klg/Desktop/frozen"),
    )
    parser.add_argument("--index", default="apache")
    parser.add_argument("--bucket", default="db_1778817827_1778817740_1", help="Small bucket key")
    parser.add_argument(
        "--large-bucket",
        default="db_1779735029_1779126165_3",
        help="Large bucket key (single comparison only)",
    )
    parser.add_argument("--iteration", default="9.9")
    parser.add_argument("--timestamp", default="20260713190000")
    parser.add_argument(
        "--large-only",
        action="store_true",
        help="Skip small-bucket matrix; run large-bucket baseline vs tuned only",
    )
    args = parser.parse_args()

    config = FrostyConfig(frozen_dir=args.frozen_dir)
    elastic_url, api_key = config.require_elastic()
    buckets = discover_buckets(config.frozen_dir)

    def resolve(name: str):
        for bucket in buckets:
            if bucket.index_name == args.index and bucket.bucket_name == name:
                return with_run_context(
                    bucket,
                    iteration=args.iteration,
                    timestamp=args.timestamp,
                )
        raise SystemExit(f"bucket not found: {args.index}/{name}")

    small = resolve(args.bucket)
    large = resolve(args.large_bucket)

    print(
        "case              batch pipeline events   read_ms decode_ms  bulk_ms  wall_ms bulk_pct docs/s_bulk"
    )

    if not args.large_only:
        for batch_size in (500, 2000, 5000):
            for pipeline in (False, True):
                label = f"small/{batch_size}/{'pipe' if pipeline else 'sync'}"
                result = _run_case(
                    small,
                    elastic_url=elastic_url,
                    api_key=api_key,
                    batch_size=batch_size,
                    pipeline=pipeline,
                )
                _print_row(label, batch_size, pipeline, result)

    # Large bucket: only baseline vs best candidate from small runs (avoid 6x full re-ingest)
    for label, batch_size, pipeline in (
        ("large/500/sync", 500, False),
        ("large/2000/sync", 2000, False),
        ("large/2000/pipe", 2000, True),
    ):
        result = _run_case(
            large,
            elastic_url=elastic_url,
            api_key=api_key,
            batch_size=batch_size,
            pipeline=pipeline,
        )
        _print_row(label, batch_size, pipeline, result)

    return 0


def _print_row(label: str, batch_size: int, pipeline: bool, result: dict) -> None:
    print(
        f"{label:17} {batch_size:5} {str(pipeline):5} {result['events']:6} "
        f"{result['read_ms']:8.1f} {result['decode_ms']:9.1f} {result['bulk_ms']:8.1f} "
        f"{result['wall_ms']:8.1f} {result['bulk_pct']:7.1f} {result['docs_per_bulk_s']:10.1f}"
    )


if __name__ == "__main__":
    sys.exit(main())
