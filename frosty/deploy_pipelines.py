#!/usr/bin/env python3
"""Scan journals, deploy detected ingest pipelines, and attach routers to indices."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from frosty.client import FrostyClient
from frosty.config import DEFAULT_ELASTIC_URL, DEFAULT_FROZEN_DIR, FrostyConfig
from frosty.errors import FrostyConfigError, FrostyError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan frosty journals, deploy parsers for detected event kinds, "
        "and attach router pipelines to indices."
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
        "--scan-only",
        action="store_true",
        help="Scan journals and print detected event kinds without deploying pipelines",
    )
    parser.add_argument(
        "--write-json",
        action="store_true",
        help="Write generated pipeline JSON files to the pipelines/ directory",
    )
    parser.add_argument(
        "--reindex",
        action="store_true",
        help="Reindex existing documents through the router pipeline",
    )
    parser.add_argument(
        "--skip-default-pipeline",
        action="store_true",
        help="Deploy pipelines but do not set the router as the index default",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = FrostyConfig(
        frozen_dir=args.frozen_dir,
        elastic_url=args.elastic_url or DEFAULT_ELASTIC_URL,
        api_key=args.api_key,
    )
    client = FrostyClient(config)

    try:
        scan = client.scan(indices=args.indices)
    except FileNotFoundError as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)

    print(f"Scanning {len(scan.buckets)} bucket(s) across {len(scan.indices)} index(es)...")
    for index_profile in scan.indices:
        print(f"index={index_profile.index_name}")
        print(f"  event_kinds={[k.value for k in index_profile.event_kinds]}")
        for kind, count in index_profile.profile.by_kind.most_common():
            print(f"  {kind.value}: {count:,}")
        for st, count in index_profile.profile.by_sourcetype.most_common(10):
            print(f"  sourcetype {st!r}: {count:,}")
        print(f"  router_pipeline={index_profile.router_pipeline}")
        print()

    if args.scan_only:
        return

    try:
        info = client.verify_elastic()
        print(f"Connected to Elasticsearch {info.get('version', {}).get('number', 'unknown')}")

        write_json = None
        if args.write_json:
            write_json = Path(__file__).resolve().parent.parent / "pipelines"

        setup = client.setup_pipelines(
            indices=args.indices,
            write_json=write_json,
            set_default=not args.skip_default_pipeline,
            reindex=args.reindex,
        )
    except FrostyConfigError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    except FrostyError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    for pipeline_name in setup.deployed_pipelines:
        print(f"pipeline_deployed={pipeline_name}")

    for index_name, router in setup.index_routers.items():
        if not args.skip_default_pipeline:
            print(f"default_pipeline_set index=frosty-{index_name} pipeline={router}")

    for index_name, resp in setup.reindex_results.items():
        print(
            f"reindex_complete index=frosty-{index_name} "
            f"total={resp.get('total', 0)} created={resp.get('created', 0)}"
        )

    print("Done.")


if __name__ == "__main__":
    main()
