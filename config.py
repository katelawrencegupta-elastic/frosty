"""Frosty client configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from frosty.errors import FrostyConfigError

DEFAULT_ELASTIC_URL = "https://klgfrozendata-b0121e.es.us-central1.gcp.elastic.cloud:443"
DEFAULT_FROZEN_DIR = "/Users/klg/Desktop/frozen"


@dataclass
class FrostyConfig:
    """Configuration for FrostyClient."""

    frozen_dir: Path = field(
        default_factory=lambda: Path(
            os.environ.get("FROSTY_FROZEN_DIR", DEFAULT_FROZEN_DIR)
        )
    )
    elastic_url: str = field(
        default_factory=lambda: os.environ.get("ELASTIC_URL", DEFAULT_ELASTIC_URL)
    )
    api_key: str | None = field(
        default_factory=lambda: os.environ.get("ELASTIC_API_KEY")
    )
    checkpoint_path: Path | None = field(
        default_factory=lambda: (
            Path(os.environ["FROSTY_CHECKPOINT_PATH"])
            if os.environ.get("FROSTY_CHECKPOINT_PATH")
            else None
        )
    )

    def __post_init__(self) -> None:
        self.frozen_dir = Path(self.frozen_dir)
        if self.checkpoint_path is None:
            self.checkpoint_path = self.frozen_dir / ".frosty-checkpoint.db"
        else:
            self.checkpoint_path = Path(self.checkpoint_path)

    def require_elastic(self) -> tuple[str, str]:
        if not self.elastic_url:
            raise FrostyConfigError("elastic_url is required")
        if not self.api_key:
            raise FrostyConfigError(
                "api_key is required (set ELASTIC_API_KEY or pass api_key=)"
            )
        return self.elastic_url, self.api_key
