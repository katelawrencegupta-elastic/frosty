"""Pydantic request/response models for the frosty API."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class JobType(str, Enum):
    INGEST = "ingest"
    SCAN = "scan"
    DRY_RUN = "dry_run"
    SETUP_PIPELINES = "setup_pipelines"


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class IngestJobRequest(BaseModel):
    indices: list[str] | None = None
    bucket_names: list[str] | None = None
    batch_size: int = Field(default=500, ge=1, le=5000)
    workers: int = Field(default=1, ge=1, le=16)
    skip_index_create: bool = False
    resume: bool = True
    force: bool = False


class ScanJobRequest(BaseModel):
    indices: list[str] | None = None


class DryRunJobRequest(BaseModel):
    indices: list[str] | None = None
    bucket_names: list[str] | None = None


class SetupPipelinesRequest(BaseModel):
    indices: list[str] | None = None
    set_default: bool = True
    reindex: bool = False


class HealthResponse(BaseModel):
    status: str
    version: str
    elastic_configured: bool
    apm_enabled: bool


class JobCreatedResponse(BaseModel):
    job_id: str
    job_type: JobType
    status: JobStatus


class JobResponse(BaseModel):
    job_id: str
    job_type: JobType
    status: JobStatus
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    result: dict[str, Any] | None = None


class BucketResponse(BaseModel):
    index_name: str
    bucket_name: str
    bucket_key: str
    elastic_index: str
    journal_path: str
    latest_epoch: int
    earliest_epoch: int


class BucketListResponse(BaseModel):
    buckets: list[BucketResponse]
    count: int
