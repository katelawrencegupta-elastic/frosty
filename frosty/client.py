"""High-level programmatic API for frosty."""

from __future__ import annotations

import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Iterator

from frosty.buckets import FrozenBucket, discover_buckets
from frosty.checkpoint import CheckpointStore
from frosty.config import FrostyConfig
from frosty.elastic import (
    bulk_index,
    ensure_index,
    put_pipeline,
    reindex_with_pipeline,
    set_default_pipeline,
    verify_cluster,
)
from frosty.errors import FrostyConfigError, FrostyElasticError
from frosty.event_types import (
    classify_event,
    profile_buckets,
    profile_journal,
    required_parser_pipelines,
    router_pipeline_name,
)
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
from frosty.pipelines import pipelines_for_index
from frosty.worker import ingest_bucket_task

__version__ = "0.3.0"


class FrostyClient:
    """Programmatic interface for decoding frozen Splunk journals and ingesting to Elasticsearch."""

    def __init__(self, config: FrostyConfig | None = None):
        self.config = config or FrostyConfig()

    def list_buckets(
        self,
        *,
        indices: list[str] | None = None,
        bucket_names: list[str] | None = None,
    ) -> list[FrozenBucket]:
        buckets = discover_buckets(self.config.frozen_dir)
        if indices:
            allowed = set(indices)
            buckets = [b for b in buckets if b.index_name in allowed]
        if bucket_names:
            allowed_buckets = set(bucket_names)
            buckets = [b for b in buckets if b.bucket_name in allowed_buckets]
        return buckets

    def decode_journal(self, journal_path: Path | str) -> Iterator[dict]:
        """Yield raw decoded journal events (pre-Elasticsearch mapping)."""
        yield from iter_journal_events(Path(journal_path))

    def decode_bucket(self, bucket: FrozenBucket) -> Iterator[dict]:
        """Yield Elasticsearch-ready documents for all events in a bucket."""
        yield from iter_bucket_docs(bucket)

    def classify(
        self,
        *,
        sourcetype: str = "",
        source: str = "",
        message: str = "",
        splunk_index: str = "",
    ):
        return classify_event(
            sourcetype=sourcetype,
            source=source,
            message=message,
            splunk_index=splunk_index,
        )

    def scan(self, *, indices: list[str] | None = None) -> ScanResult:
        """Scan journals and return event kind profiles per Splunk index."""
        buckets = self.list_buckets(indices=indices)
        if not buckets:
            raise FileNotFoundError(f"No buckets found under {self.config.frozen_dir}")

        index_buckets: dict[str, list[FrozenBucket]] = {}
        for bucket in buckets:
            index_buckets.setdefault(bucket.index_name, []).append(bucket)

        profiles: list[IndexScanProfile] = []
        for index_name, index_bucket_list in sorted(index_buckets.items()):
            profile = profile_buckets(index_bucket_list)
            kinds = required_parser_pipelines(profile)
            profiles.append(
                IndexScanProfile(
                    index_name=index_name,
                    profile=profile,
                    event_kinds=kinds,
                    router_pipeline=router_pipeline_name(index_name),
                )
            )

        return ScanResult(buckets=buckets, indices=profiles)

    def dry_run(
        self,
        *,
        indices: list[str] | None = None,
        bucket_names: list[str] | None = None,
    ) -> DryRunResult:
        """Decode buckets and count events without contacting Elasticsearch."""
        buckets = self.list_buckets(indices=indices, bucket_names=bucket_names)
        if not buckets:
            raise FileNotFoundError(f"No buckets found under {self.config.frozen_dir}")

        profile = profile_buckets(buckets)
        counts: list[BucketCount] = []
        total = 0
        for bucket in buckets:
            event_count = sum(1 for _ in iter_bucket_docs(bucket))
            counts.append(BucketCount(bucket=bucket, event_count=event_count))
            total += event_count

        return DryRunResult(buckets=counts, profile=profile, total_events=total)

    def ingest(
        self,
        *,
        indices: list[str] | None = None,
        bucket_names: list[str] | None = None,
        batch_size: int = 500,
        workers: int = 1,
        skip_index_create: bool = False,
        resume: bool = True,
        force: bool = False,
        checkpoint_path: Path | None = None,
    ) -> IngestResult:
        """Decode and bulk-ingest buckets into Elasticsearch."""
        buckets = self.list_buckets(indices=indices, bucket_names=bucket_names)
        if not buckets:
            raise FileNotFoundError(f"No buckets found under {self.config.frozen_dir}")

        ckpt_path = checkpoint_path or self.config.checkpoint_path
        checkpoint = CheckpointStore(ckpt_path) if ckpt_path else None

        if force and checkpoint is not None:
            checkpoint.clear()

        result = IngestResult()
        pending: list[FrozenBucket] = []

        for bucket in buckets:
            if resume and checkpoint is not None and checkpoint.is_completed(bucket.bucket_key):
                record = checkpoint.get(bucket.bucket_key)
                result.buckets.append(
                    BucketIngestResult(
                        bucket_key=bucket.bucket_key,
                        index_name=bucket.index_name,
                        bucket_name=bucket.bucket_name,
                        elastic_index=bucket.elastic_index,
                        indexed=record.indexed if record else 0,
                        errors=record.errors if record else 0,
                        status="skipped",
                    )
                )
            else:
                pending.append(bucket)

        if not pending:
            if checkpoint is not None:
                checkpoint.close()
            return result

        elastic_url, api_key = self.config.require_elastic()
        verify_cluster(elastic_url, api_key)

        if not skip_index_create:
            seen: set[str] = set()
            for bucket in pending:
                if bucket.elastic_index not in seen:
                    ensure_index(elastic_url, api_key, bucket.elastic_index)
                    seen.add(bucket.elastic_index)

        workers = max(1, min(workers, len(pending)))

        if workers == 1:
            for bucket in pending:
                result.buckets.append(
                    self._ingest_one_bucket(
                        bucket, elastic_url, api_key, batch_size, checkpoint
                    )
                )
        else:
            result.buckets.extend(
                self._ingest_parallel(pending, elastic_url, api_key, batch_size, workers, checkpoint)
            )

        if checkpoint is not None:
            checkpoint.close()
        return result

    def _ingest_one_bucket(
        self,
        bucket: FrozenBucket,
        elastic_url: str,
        api_key: str,
        batch_size: int,
        checkpoint: CheckpointStore | None,
    ) -> BucketIngestResult:
        if checkpoint is not None:
            checkpoint.mark_in_progress(bucket.bucket_key, bucket.index_name, bucket.bucket_name)
        try:
            indexed, errors = bulk_index(
                elastic_url,
                api_key,
                bucket.elastic_index,
                iter_bucket_docs(bucket),
                batch_size=batch_size,
            )
            status = "completed"
            if checkpoint is not None:
                checkpoint.mark_completed(
                    bucket.bucket_key, bucket.index_name, bucket.bucket_name, indexed, errors
                )
        except Exception:
            if checkpoint is not None:
                checkpoint.mark_failed(bucket.bucket_key, bucket.index_name, bucket.bucket_name, 0, 0)
            raise FrostyElasticError(f"Ingest failed for {bucket.bucket_key}")

        return BucketIngestResult(
            bucket_key=bucket.bucket_key,
            index_name=bucket.index_name,
            bucket_name=bucket.bucket_name,
            elastic_index=bucket.elastic_index,
            indexed=indexed,
            errors=errors,
            status=status,
        )

    def _ingest_parallel(
        self,
        buckets: list[FrozenBucket],
        elastic_url: str,
        api_key: str,
        batch_size: int,
        workers: int,
        checkpoint: CheckpointStore | None,
    ) -> list[BucketIngestResult]:
        results: list[BucketIngestResult] = []
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {}
            for bucket in buckets:
                if checkpoint is not None:
                    checkpoint.mark_in_progress(
                        bucket.bucket_key, bucket.index_name, bucket.bucket_name
                    )
                future = pool.submit(
                    ingest_bucket_task, bucket, elastic_url, api_key, batch_size
                )
                futures[future] = bucket

            for future in as_completed(futures):
                bucket = futures[future]
                try:
                    indexed, errors = future.result()
                    status = "completed"
                    if checkpoint is not None:
                        checkpoint.mark_completed(
                            bucket.bucket_key,
                            bucket.index_name,
                            bucket.bucket_name,
                            indexed,
                            errors,
                        )
                except Exception as exc:
                    status = "failed"
                    indexed, errors = 0, 0
                    if checkpoint is not None:
                        checkpoint.mark_failed(
                            bucket.bucket_key,
                            bucket.index_name,
                            bucket.bucket_name,
                            0,
                            0,
                        )
                    results.append(
                        BucketIngestResult(
                            bucket_key=bucket.bucket_key,
                            index_name=bucket.index_name,
                            bucket_name=bucket.bucket_name,
                            elastic_index=bucket.elastic_index,
                            indexed=0,
                            errors=0,
                            status="failed",
                        )
                    )
                    continue

                results.append(
                    BucketIngestResult(
                        bucket_key=bucket.bucket_key,
                        index_name=bucket.index_name,
                        bucket_name=bucket.bucket_name,
                        elastic_index=bucket.elastic_index,
                        indexed=indexed,
                        errors=errors,
                        status=status,
                    )
                )
        return results

    def setup_pipelines(
        self,
        *,
        indices: list[str] | None = None,
        write_json: Path | None = None,
        set_default: bool = True,
        reindex: bool = False,
    ) -> PipelineSetupResult:
        """Scan journals, deploy detected parser pipelines, and attach router pipelines."""
        elastic_url, api_key = self.config.require_elastic()
        scan = self.scan(indices=indices)
        verify_cluster(elastic_url, api_key)

        setup = PipelineSetupResult()
        deployed: set[str] = set()

        if write_json is not None:
            write_json.mkdir(parents=True, exist_ok=True)

        for index_profile in scan.indices:
            index_name = index_profile.index_name
            kinds = index_profile.event_kinds
            elastic_index = f"frosty-{index_name}"
            pipeline_defs = pipelines_for_index(kinds, index_name)

            for pipeline_name, payload in pipeline_defs.items():
                if pipeline_name in deployed:
                    continue
                put_pipeline(elastic_url, api_key, pipeline_name, payload)
                deployed.add(pipeline_name)
                setup.deployed_pipelines.append(pipeline_name)

                if write_json is not None:
                    path = write_json / f"{pipeline_name}.json"
                    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

            router = router_pipeline_name(index_name)
            setup.index_routers[index_name] = router

            if set_default:
                set_default_pipeline(elastic_url, api_key, elastic_index, router)

            if reindex:
                resp = reindex_with_pipeline(elastic_url, api_key, elastic_index, router)
                setup.reindex_results[index_name] = resp

        return setup

    def journal_profile(self, journal_path: Path | str, *, splunk_index: str = ""):
        return profile_journal(Path(journal_path), splunk_index=splunk_index)

    def verify_elastic(self) -> dict:
        elastic_url, api_key = self.config.require_elastic()
        return verify_cluster(elastic_url, api_key)

    def clear_checkpoint(self, bucket_key: str | None = None) -> None:
        if self.config.checkpoint_path is None:
            raise FrostyConfigError("checkpoint_path is not configured")
        store = CheckpointStore(self.config.checkpoint_path)
        try:
            store.clear(bucket_key)
        finally:
            store.close()
