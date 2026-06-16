"""FastAPI application for the frosty thaw/ingest service."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Security, status
from fastapi.security import APIKeyHeader

from frosty.api.apm import apm_config_from_env, setup_apm
from frosty.api.jobs import Job, JobManager
from frosty.api.schemas import (
    BucketListResponse,
    BucketResponse,
    DryRunJobRequest,
    HealthResponse,
    IngestJobRequest,
    JobCreatedResponse,
    JobResponse,
    JobType,
    ScanJobRequest,
    SetupPipelinesRequest,
)
from frosty.api.serializers import (
    dry_run_to_dict,
    ingest_result_to_dict,
    pipeline_setup_to_dict,
    scan_result_to_dict,
)
from frosty.client import FrostyClient, __version__
from frosty.config import FrostyConfig
from frosty.errors import FrostyConfigError, FrostyError

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def require_api_key(
    api_key: Annotated[str | None, Security(_api_key_header)] = None,
) -> None:
    expected = os.environ.get("FROSTY_API_KEY")
    if expected and api_key != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-API-Key header",
        )


def _job_to_response(job: Job) -> JobResponse:
    return JobResponse(
        job_id=job.job_id,
        job_type=job.job_type,
        status=job.status,
        created_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        error=job.error,
        result=job.result,
    )


def create_app(config: FrostyConfig | None = None) -> FastAPI:
    frosty_config = config or FrostyConfig()
    client = FrostyClient(frosty_config)
    job_manager = JobManager(max_workers=int(os.environ.get("FROSTY_JOB_WORKERS", "2")))
    apm_enabled = apm_config_from_env() is not None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        job_manager.shutdown()

    app = FastAPI(
        title="Frosty API",
        description="Decode Splunk frozen journal.zst buckets and ingest into Elasticsearch",
        version=__version__,
        lifespan=lifespan,
    )

    if apm_enabled:
        setup_apm(app)

    def _submit(job_type: JobType, fn) -> JobCreatedResponse:
        job = job_manager.submit(job_type, fn)
        return JobCreatedResponse(job_id=job.job_id, job_type=job.job_type, status=job.status)

    @app.get("/health", response_model=HealthResponse, tags=["meta"])
    def health() -> HealthResponse:
        return HealthResponse(
            status="ok",
            version=__version__,
            elastic_configured=bool(frosty_config.api_key and frosty_config.elastic_url),
            apm_enabled=apm_enabled,
        )

    @app.get("/v1/buckets", response_model=BucketListResponse, tags=["buckets"])
    def list_buckets(
        index: list[str] | None = None,
        _: None = Depends(require_api_key),
    ) -> BucketListResponse:
        try:
            buckets = client.list_buckets(indices=index)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        items = [
            BucketResponse(
                index_name=b.index_name,
                bucket_name=b.bucket_name,
                bucket_key=b.bucket_key,
                elastic_index=b.elastic_index,
                journal_path=str(b.journal_path),
                latest_epoch=b.latest_epoch,
                earliest_epoch=b.earliest_epoch,
            )
            for b in buckets
        ]
        return BucketListResponse(buckets=items, count=len(items))

    @app.post(
        "/v1/jobs/scan",
        response_model=JobCreatedResponse,
        status_code=status.HTTP_202_ACCEPTED,
        tags=["jobs"],
    )
    def submit_scan(
        body: ScanJobRequest,
        _: None = Depends(require_api_key),
    ) -> JobCreatedResponse:
        return _submit(
            JobType.SCAN,
            lambda: scan_result_to_dict(client.scan(indices=body.indices)),
        )

    @app.post(
        "/v1/jobs/dry-run",
        response_model=JobCreatedResponse,
        status_code=status.HTTP_202_ACCEPTED,
        tags=["jobs"],
    )
    def submit_dry_run(
        body: DryRunJobRequest,
        _: None = Depends(require_api_key),
    ) -> JobCreatedResponse:
        return _submit(
            JobType.DRY_RUN,
            lambda: dry_run_to_dict(
                client.dry_run(indices=body.indices, bucket_names=body.bucket_names)
            ),
        )

    @app.post(
        "/v1/jobs/ingest",
        response_model=JobCreatedResponse,
        status_code=status.HTTP_202_ACCEPTED,
        tags=["jobs"],
    )
    def submit_ingest(
        body: IngestJobRequest,
        _: None = Depends(require_api_key),
    ) -> JobCreatedResponse:
        def _run() -> dict:
            try:
                result = client.ingest(
                    indices=body.indices,
                    bucket_names=body.bucket_names,
                    batch_size=body.batch_size,
                    workers=body.workers,
                    skip_index_create=body.skip_index_create,
                    resume=body.resume,
                    force=body.force,
                )
            except (FrostyConfigError, FrostyError) as exc:
                raise RuntimeError(str(exc)) from exc
            return ingest_result_to_dict(result)

        return _submit(JobType.INGEST, _run)

    @app.post(
        "/v1/jobs/pipelines/setup",
        response_model=JobCreatedResponse,
        status_code=status.HTTP_202_ACCEPTED,
        tags=["jobs"],
    )
    def submit_setup_pipelines(
        body: SetupPipelinesRequest,
        _: None = Depends(require_api_key),
    ) -> JobCreatedResponse:
        def _run() -> dict:
            try:
                result = client.setup_pipelines(
                    indices=body.indices,
                    set_default=body.set_default,
                    reindex=body.reindex,
                )
            except (FrostyConfigError, FrostyError) as exc:
                raise RuntimeError(str(exc)) from exc
            return pipeline_setup_to_dict(result)

        return _submit(JobType.SETUP_PIPELINES, _run)

    @app.get("/v1/jobs", response_model=list[JobResponse], tags=["jobs"])
    def list_jobs(
        limit: int = 50,
        _: None = Depends(require_api_key),
    ) -> list[JobResponse]:
        return [_job_to_response(job) for job in job_manager.list_jobs(limit=limit)]

    @app.get("/v1/jobs/{job_id}", response_model=JobResponse, tags=["jobs"])
    def get_job(
        job_id: str,
        _: None = Depends(require_api_key),
    ) -> JobResponse:
        job = job_manager.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
        return _job_to_response(job)

    @app.post("/v1/elastic/verify", tags=["elastic"])
    def verify_elastic(_: None = Depends(require_api_key)) -> dict:
        try:
            info = client.verify_elastic()
        except FrostyConfigError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except FrostyError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        return {
            "connected": True,
            "version": info.get("version", {}).get("number", "unknown"),
            "cluster_name": info.get("cluster_name"),
        }

    return app
