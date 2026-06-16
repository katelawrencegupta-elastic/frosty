"""Bulk ingest documents into Elasticsearch."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Iterable, Iterator

from frosty.errors import FrostyElasticError


def elastic_request(
    elastic_url: str,
    api_key: str,
    method: str,
    path: str,
    body: bytes | str | None = None,
    *,
    content_type: str = "application/json",
    timeout: int = 120,
    retries: int = 5,
) -> tuple[int, dict]:
    url = f"{elastic_url.rstrip('/')}{path}"
    headers = {
        "Authorization": f"ApiKey {api_key}",
        "Content-Type": content_type,
    }
    data = None
    if body is not None:
        data = body.encode("utf-8") if isinstance(body, str) else body

    last_error: Exception | None = None
    for attempt in range(retries):
        request = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8")
                return response.status, json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            try:
                payload = json.loads(raw) if raw else {"error": raw}
            except json.JSONDecodeError:
                payload = {"error": raw}
            if exc.code in (429, 502, 503, 504) and attempt < retries - 1:
                time.sleep(2**attempt)
                continue
            return exc.code, payload
        except (urllib.error.URLError, TimeoutError, BrokenPipeError, OSError) as exc:
            last_error = exc
            if attempt < retries - 1:
                time.sleep(2**attempt)
                continue
            raise

    raise last_error or RuntimeError("elastic_request failed")


def verify_cluster(elastic_url: str, api_key: str) -> dict:
    status, resp = elastic_request(elastic_url, api_key, "GET", "/")
    if status != 200:
        raise FrostyElasticError(
            f"Elasticsearch connection failed: status={status}",
            status=status,
            response=resp,
        )
    return resp


def ensure_index(elastic_url: str, api_key: str, index_name: str) -> None:
    status, resp = elastic_request(elastic_url, api_key, "HEAD", f"/{index_name}")
    if status == 200:
        return
    if status != 404:
        raise FrostyElasticError(
            f"Index check failed for {index_name}: status={status}",
            status=status,
            response=resp,
        )

    create_body = {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "message": {"type": "text"},
                "host": {"type": "keyword"},
                "source": {"type": "keyword"},
                "sourcetype": {"type": "keyword"},
                "event": {
                    "properties": {
                        "kind": {"type": "keyword"},
                        "dataset": {"type": "keyword"},
                        "category": {"type": "keyword"},
                        "original": {"type": "text"},
                    }
                },
                "splunk.pipeline": {"type": "keyword"},
                "splunk.classify_reason": {"type": "keyword"},
                "splunk.index": {"type": "keyword"},
                "splunk.index_time": {"type": "date", "format": "epoch_second"},
                "splunk.bucket_name": {"type": "keyword"},
                "splunk.bucket_latest": {"type": "long"},
                "splunk.bucket_earliest": {"type": "long"},
                "splunk.fields": {"type": "object", "enabled": True},
            }
        },
    }
    status, resp = elastic_request(
        elastic_url, api_key, "PUT", f"/{index_name}", json.dumps(create_body)
    )
    if status not in (200, 201):
        raise FrostyElasticError(
            f"Index create failed for {index_name}: status={status}",
            status=status,
            response=resp,
        )


def put_pipeline(
    elastic_url: str,
    api_key: str,
    pipeline_name: str,
    pipeline_payload: dict,
) -> None:
    status, resp = elastic_request(
        elastic_url,
        api_key,
        "PUT",
        f"/_ingest/pipeline/{pipeline_name}",
        json.dumps(pipeline_payload),
    )
    if status != 200:
        raise FrostyElasticError(
            f"pipeline_put_failed name={pipeline_name} status={status}",
            status=status,
            response=resp,
        )


def set_default_pipeline(elastic_url: str, api_key: str, index_name: str, pipeline_name: str) -> None:
    status, resp = elastic_request(
        elastic_url,
        api_key,
        "PUT",
        f"/{index_name}/_settings",
        json.dumps({"index": {"default_pipeline": pipeline_name}}),
    )
    if status != 200:
        raise FrostyElasticError(
            f"default_pipeline_set_failed index={index_name} status={status}",
            status=status,
            response=resp,
        )


def reindex_with_pipeline(
    elastic_url: str,
    api_key: str,
    source_index: str,
    pipeline_name: str,
    *,
    timeout: int = 600,
) -> dict:
    """Reindex an index through a pipeline using a temporary destination index."""
    temp_index = f"{source_index}-reindex-tmp"
    elastic_request(elastic_url, api_key, "DELETE", f"/{temp_index}")

    body = {
        "source": {"index": source_index},
        "dest": {"index": temp_index, "pipeline": pipeline_name},
    }
    status, resp = elastic_request(
        elastic_url,
        api_key,
        "POST",
        "/_reindex?wait_for_completion=true",
        json.dumps(body),
        timeout=timeout,
    )
    if status != 200:
        raise FrostyElasticError(
            f"reindex_failed source={source_index} status={status}",
            status=status,
            response=resp,
        )

    elastic_request(elastic_url, api_key, "DELETE", f"/{source_index}")

    copy_body = {"source": {"index": temp_index}, "dest": {"index": source_index}}
    status, resp = elastic_request(
        elastic_url,
        api_key,
        "POST",
        "/_reindex?wait_for_completion=true",
        json.dumps(copy_body),
        timeout=timeout,
    )
    if status != 200:
        raise FrostyElasticError(
            f"reindex_rename_failed temp={temp_index} dest={source_index} status={status}",
            status=status,
            response=resp,
        )

    elastic_request(elastic_url, api_key, "DELETE", f"/{temp_index}")
    set_default_pipeline(elastic_url, api_key, source_index, pipeline_name)
    return resp


def bulk_lines(index_name: str, docs: Iterable[dict]) -> Iterator[str]:
    for doc in docs:
        yield json.dumps({"index": {"_index": index_name}}, separators=(",", ":"))
        yield json.dumps(doc, separators=(",", ":"))


def bulk_index(
    elastic_url: str,
    api_key: str,
    index_name: str,
    docs: Iterable[dict],
    *,
    batch_size: int = 500,
) -> tuple[int, int]:
    """Bulk index documents. Returns (indexed_count, error_count)."""
    indexed = 0
    errors = 0
    batch: list[str] = []

    def flush() -> None:
        nonlocal indexed, errors, batch
        if not batch:
            return
        body = "\n".join(batch) + "\n"
        status, resp = elastic_request(
            elastic_url,
            api_key,
            "POST",
            "/_bulk",
            body=body,
            content_type="application/x-ndjson",
            timeout=300,
        )
        if status != 200:
            raise FrostyElasticError(
                f"Bulk request failed: status={status}",
                status=status,
                response=resp,
            )

        if resp.get("errors"):
            for item in resp.get("items", []):
                action = item.get("index") or item.get("create") or {}
                if action.get("error"):
                    errors += 1
                else:
                    indexed += 1
        else:
            indexed += len(batch) // 2
        batch = []

    for line in bulk_lines(index_name, docs):
        batch.append(line)
        if len(batch) >= batch_size * 2:
            flush()

    flush()
    return indexed, errors
