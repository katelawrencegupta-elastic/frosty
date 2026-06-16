"""Frosty library exceptions."""


class FrostyError(Exception):
    """Base error for frosty operations."""


class FrostyConfigError(FrostyError):
    """Invalid or incomplete configuration."""


class FrostyElasticError(FrostyError):
    """Elasticsearch request failed."""

    def __init__(self, message: str, *, status: int | None = None, response: dict | None = None):
        super().__init__(message)
        self.status = status
        self.response = response or {}
