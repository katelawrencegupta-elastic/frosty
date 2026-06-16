"""SQLite checkpoint store for resumable bucket ingest."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class CheckpointRecord:
    bucket_key: str
    index_name: str
    bucket_name: str
    status: str
    indexed: int
    errors: int
    updated_at: str


class CheckpointStore:
    """Track per-bucket ingest progress for resume support."""

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path, timeout=30)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bucket_checkpoint (
                bucket_key TEXT PRIMARY KEY,
                index_name TEXT NOT NULL,
                bucket_name TEXT NOT NULL,
                status TEXT NOT NULL,
                indexed INTEGER NOT NULL DEFAULT 0,
                errors INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            )
            """
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def get(self, bucket_key: str) -> CheckpointRecord | None:
        row = self._conn.execute(
            "SELECT bucket_key, index_name, bucket_name, status, indexed, errors, updated_at "
            "FROM bucket_checkpoint WHERE bucket_key = ?",
            (bucket_key,),
        ).fetchone()
        if row is None:
            return None
        return CheckpointRecord(*row)

    def is_completed(self, bucket_key: str) -> bool:
        record = self.get(bucket_key)
        return record is not None and record.status == "completed"

    def mark_in_progress(self, bucket_key: str, index_name: str, bucket_name: str) -> None:
        self._upsert(bucket_key, index_name, bucket_name, "in_progress", 0, 0)

    def mark_completed(
        self, bucket_key: str, index_name: str, bucket_name: str, indexed: int, errors: int
    ) -> None:
        self._upsert(bucket_key, index_name, bucket_name, "completed", indexed, errors)

    def mark_failed(
        self, bucket_key: str, index_name: str, bucket_name: str, indexed: int, errors: int
    ) -> None:
        self._upsert(bucket_key, index_name, bucket_name, "failed", indexed, errors)

    def clear(self, bucket_key: str | None = None) -> None:
        if bucket_key is None:
            self._conn.execute("DELETE FROM bucket_checkpoint")
        else:
            self._conn.execute(
                "DELETE FROM bucket_checkpoint WHERE bucket_key = ?", (bucket_key,)
            )
        self._conn.commit()

    def _upsert(
        self,
        bucket_key: str,
        index_name: str,
        bucket_name: str,
        status: str,
        indexed: int,
        errors: int,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            """
            INSERT INTO bucket_checkpoint
                (bucket_key, index_name, bucket_name, status, indexed, errors, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bucket_key) DO UPDATE SET
                status = excluded.status,
                indexed = excluded.indexed,
                errors = excluded.errors,
                updated_at = excluded.updated_at
            """,
            (bucket_key, index_name, bucket_name, status, indexed, errors, now),
        )
        self._conn.commit()
