FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    FROSTY_FROZEN_DIR=/data/frozen \
    FROSTY_CHECKPOINT_PATH=/data/checkpoint/.frosty-checkpoint.db \
    FROSTY_API_HOST=0.0.0.0 \
    FROSTY_API_PORT=8080

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl docker-cli \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY frosty ./frosty

RUN pip install --upgrade pip \
    && pip install ".[api]" psutil

RUN useradd --create-home --shell /usr/sbin/nologin frosty \
    && mkdir -p /data/frozen /data/checkpoint \
    && chown -R frosty:frosty /app /data

USER frosty

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS "http://127.0.0.1:${FROSTY_API_PORT}/health" || exit 1

CMD ["frosty-api"]
