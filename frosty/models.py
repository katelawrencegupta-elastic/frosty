"""Result dataclasses returned by FrostyClient."""

from __future__ import annotations

from dataclasses import dataclass, field

from frosty.buckets import FrozenBucket
from frosty.event_types import EventKind, EventProfile


@dataclass(frozen=True)
class BucketCount:
    bucket: FrozenBucket
    event_count: int


@dataclass
class DryRunResult:
    buckets: list[BucketCount] = field(default_factory=list)
    profile: EventProfile = field(default_factory=EventProfile)
    total_events: int = 0


@dataclass
class IndexScanProfile:
    index_name: str
    profile: EventProfile
    event_kinds: list[EventKind]
    router_pipeline: str


@dataclass
class ScanResult:
    buckets: list[FrozenBucket]
    indices: list[IndexScanProfile] = field(default_factory=list)


@dataclass(frozen=True)
class BucketIngestResult:
    bucket_key: str
    index_name: str
    bucket_name: str
    elastic_index: str
    indexed: int
    errors: int
    status: str  # completed, failed, skipped


@dataclass
class IngestResult:
    buckets: list[BucketIngestResult] = field(default_factory=list)

    @property
    def total_indexed(self) -> int:
        return sum(b.indexed for b in self.buckets if b.status == "completed")

    @property
    def total_errors(self) -> int:
        return sum(b.errors for b in self.buckets)

    @property
    def skipped(self) -> int:
        return sum(1 for b in self.buckets if b.status == "skipped")

    @property
    def failed(self) -> int:
        return sum(1 for b in self.buckets if b.status == "failed")


@dataclass
class PipelineSetupResult:
    deployed_pipelines: list[str] = field(default_factory=list)
    index_routers: dict[str, str] = field(default_factory=dict)
    reindex_results: dict[str, dict] = field(default_factory=dict)
