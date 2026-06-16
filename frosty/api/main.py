"""Uvicorn entrypoint for the frosty API service."""

from __future__ import annotations

import os

import uvicorn

from frosty.api.app import create_app


def run() -> None:
    host = os.environ.get("FROSTY_API_HOST", "0.0.0.0")
    port = int(os.environ.get("FROSTY_API_PORT", "8080"))
    reload = os.environ.get("FROSTY_API_RELOAD", "").lower() in ("1", "true", "yes")
    uvicorn.run(
        "frosty.api.app:create_app",
        factory=True,
        host=host,
        port=port,
        reload=reload,
    )


if __name__ == "__main__":
    run()
