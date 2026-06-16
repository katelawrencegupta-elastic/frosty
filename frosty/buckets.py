"""Discover Splunk frozen bucket directories."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

BUCKET_RE = re.compile(r"^db_(?P<latest>\d+)_(?P<earliest>\d+)_(?P<seq>\d+)$")


@dataclass(frozen=True)
class FrozenBucket:
    """A frozen Splunk bucket with its journal file."""

    index_name: str
    bucket_name: str
    latest_epoch: int
    earliest_epoch: int
    sequence: int
    journal_path: Path

    @property
    def elastic_index(self) -> str:
        return f"frosty-{self.index_name}"

    @property
    def bucket_key(self) -> str:
        return f"{self.index_name}/{self.bucket_name}"


def discover_buckets(frozen_root: Path) -> list[FrozenBucket]:
    """Walk frozen_root and return buckets that contain rawdata/journal.zst."""
    buckets: list[FrozenBucket] = []
    if not frozen_root.is_dir():
        raise FileNotFoundError(f"Frozen root not found: {frozen_root}")

    for index_dir in sorted(frozen_root.iterdir()):
        if not index_dir.is_dir():
            continue
        index_name = index_dir.name
        for bucket_dir in sorted(index_dir.iterdir()):
            if not bucket_dir.is_dir():
                continue
            match = BUCKET_RE.match(bucket_dir.name)
            if not match:
                continue
            journal = bucket_dir / "rawdata" / "journal.zst"
            if not journal.is_file():
                continue
            buckets.append(
                FrozenBucket(
                    index_name=index_name,
                    bucket_name=bucket_dir.name,
                    latest_epoch=int(match.group("latest")),
                    earliest_epoch=int(match.group("earliest")),
                    sequence=int(match.group("seq")),
                    journal_path=journal,
                )
            )
    return buckets
