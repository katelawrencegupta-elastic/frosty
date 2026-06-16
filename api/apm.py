"""Elastic APM integration for the frosty FastAPI service."""

from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI


def apm_config_from_env() -> dict[str, Any] | None:
    server_url = os.environ.get("ELASTIC_APM_SERVER_URL")
    if not server_url:
        return None
    config: dict[str, Any] = {
        "SERVICE_NAME": os.environ.get("ELASTIC_APM_SERVICE_NAME", "frosty-api"),
        "SERVER_URL": server_url,
        "ENVIRONMENT": os.environ.get("ELASTIC_APM_ENVIRONMENT", "production"),
    }
    secret_token = os.environ.get("ELASTIC_APM_SECRET_TOKEN")
    if secret_token:
        config["SECRET_TOKEN"] = secret_token
    api_key = os.environ.get("ELASTIC_APM_API_KEY")
    if api_key:
        config["API_KEY"] = api_key
    return config


def setup_apm(app: FastAPI) -> bool:
    """Attach Elastic APM middleware when ELASTIC_APM_SERVER_URL is set."""
    config = apm_config_from_env()
    if not config:
        return False
    try:
        from elasticapm.contrib.starlette import ElasticAPM, make_apm_client
    except ImportError as exc:
        raise RuntimeError(
            "elastic-apm is required for APM. Install with: pip install frosty[api]"
        ) from exc
    client = make_apm_client(config)
    app.add_middleware(ElasticAPM, client=client)
    return True
