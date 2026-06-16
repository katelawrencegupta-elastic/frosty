"""Background job store and executor for the frosty API."""

from __future__ import annotations

import traceback
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Callable

from frosty.api.schemas import JobStatus, JobType


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Job:
    job_id: str
    job_type: JobType
    status: JobStatus = JobStatus.PENDING
    created_at: datetime = field(default_factory=_utcnow)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    result: dict[str, Any] | None = None


class JobManager:
    """Run long-running frosty tasks in background threads."""

    def __init__(self, max_workers: int = 2):
        self._jobs: dict[str, Job] = {}
        self._futures: dict[str, Future] = {}
        self._lock = Lock()
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="frosty-job")

    def shutdown(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)

    def list_jobs(self, limit: int = 50) -> list[Job]:
        with self._lock:
            jobs = sorted(self._jobs.values(), key=lambda j: j.created_at, reverse=True)
            return jobs[:limit]

    def submit(self, job_type: JobType, fn: Callable[[], Any]) -> Job:
        job_id = str(uuid.uuid4())
        job = Job(job_id=job_id, job_type=job_type)
        with self._lock:
            self._jobs[job_id] = job

        def _run() -> None:
            with self._lock:
                job.status = JobStatus.RUNNING
                job.started_at = _utcnow()
            try:
                outcome = fn()
                with self._lock:
                    job.status = JobStatus.COMPLETED
                    job.result = outcome if isinstance(outcome, dict) else {"value": outcome}
                    job.finished_at = _utcnow()
            except Exception as exc:
                with self._lock:
                    job.status = JobStatus.FAILED
                    job.error = str(exc)
                    job.result = {"traceback": traceback.format_exc()}
                    job.finished_at = _utcnow()

        future = self._pool.submit(_run)
        with self._lock:
            self._futures[job_id] = future
        return job
