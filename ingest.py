#!/usr/bin/env python3
"""CLI to decode frozen Splunk journals and ingest into Elasticsearch."""

from __future__ import annotations

import argparse
import sys

from frosty.client import FrostyClient
from frosty.config import DEFAULT_ELASTIC_URL, DEFAULT_FROZEN_DIR, FrostyConfig
from frosty.errors import FrostyConfigError, FrostyElasticError, FrostyError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Decode Splunk frozen journal.zst buckets and ingest into Elasticsearch."
    )
    parser.add_argument(
        "--frozen-dir",
        default=DEFAULT_FROZEN_DIR,
        help=f"Root directory containing index subfolders (default: {DEFAULT_FROZEN_DIR})",
    )
    parser.add_argument(
        "--elastic-url",
        default=None,
        help="Elasticsearch cluster URL",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="Elasticsearch API key (or set ELASTIC_API_KEY)",
    )
    parser.add_argument(
        "--index",
        dest="indices",
        action="append",
        help="Only process this Splunk index name (apache, nginx, syslog). Repeatable.",
    )
    parser.add_argument(
        "--bucket",
        dest="buckets",
        action="append",
        help="Only process this bucket directory name (e.g. db_1779735029_1779126165_3). Repeatable.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Bulk API batch size (default: 500)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Parallel bucket workers (default: 1)",
    )
    parser.add_argument(
        "--checkpoint",
        default=None,
        help="Checkpoint database path (default: <frozen-dir>/.frosty-checkpoint.db)",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Ignore checkpoint and re-ingest all buckets",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Clear checkpoint and re-ingest all buckets",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Decode and count events without sending to Elasticsearch",
    )
    parser.add_argument(
        "--skip-index-create",
        action="store_true",
        help="Do not create indices; assume they already exist",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = FrostyConfig(
        frozen_dir=args.frozen_dir,
        elastic_url=args.elastic_url or DEFAULT_ELASTIC_URL,
        api_key=args.api_key,
        checkpoint_path=args.checkpoint,
    )
    client = FrostyClient(config)

    try:
        buckets = client.list_buckets(indices=args.indices, bucket_names=args.buckets)
    except FileNotFoundError as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)

    if not buckets:
        print(f"No journal.zst buckets found under {config.frozen_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(buckets)} bucket(s):")
    for bucket in buckets:
        print(f"  {bucket.index_name}/{bucket.bucket_name} -> {bucket.journal_path}")

    if args.dry_run:
        result = client.dry_run(indices=args.indices, bucket_names=args.buckets)
        for item in result.buckets:
            b = item.bucket
            print(f"  {b.index_name}/{b.bucket_name}: {item.event_count} events")
        print("Event kinds detected:")
        for kind, count in result.profile.by_kind.most_common():
            print(f"  {kind.value}: {count:,}")
        print(f"Dry run complete: {result.total_events} total events")
        return

    try:
        info = client.verify_elastic()
        print(f"Connected to Elasticsearch {info.get('version', {}).get('number', 'unknown')}")

        result = client.ingest(
            indices=args.indices,
            bucket_names=args.buckets,
            batch_size=args.batch_size,
            workers=args.workers,
            skip_index_create=args.skip_index_create,
            resume=not args.no_resume,
            force=args.force,
        )
    except FrostyConfigError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    except FrostyError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    for bucket_result in result.buckets:
        print(
            f"  {bucket_result.bucket_key} -> {bucket_result.elastic_index} "
            f"status={bucket_result.status} indexed={bucket_result.indexed} errors={bucket_result.errors}"
        )

    print(
        f"Done: {result.total_indexed} documents indexed, "
        f"{result.total_errors} errors, {result.skipped} skipped, {result.failed} failed"
    )
    if result.failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
