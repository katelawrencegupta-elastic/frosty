"""Public frosty API."""

from frosty.buckets import FrozenBucket, discover_buckets
from frosty.client import FrostyClient, __version__
from frosty.config import FrostyConfig, DEFAULT_ELASTIC_URL, DEFAULT_FROZEN_DIR
from frosty.errors import FrostyConfigError, FrostyElasticError, FrostyError
from frosty.event_types import ClassifiedEvent, EventKind, EventProfile, classify_event
from frosty.journal import iter_bucket_docs, iter_journal_events
from frosty.models import (
    BucketCount,
    BucketIngestResult,
    DryRunResult,
    IndexScanProfile,
    IngestResult,
    PipelineSetupResult,
    ScanResult,
)

__all__ = [
    "__version__",
    "FrostyClient",
    "FrostyConfig",
    "FrostyError",
    "FrostyConfigError",
    "FrostyElasticError",
    "FrozenBucket",
    "discover_buckets",
    "classify_event",
    "ClassifiedEvent",
    "EventKind",
    "EventProfile",
    "iter_journal_events",
    "iter_bucket_docs",
    "ScanResult",
    "DryRunResult",
    "IngestResult",
    "BucketIngestResult",
    "BucketCount",
    "IndexScanProfile",
    "PipelineSetupResult",
    "DEFAULT_ELASTIC_URL",
    "DEFAULT_FROZEN_DIR",
]
