#!/usr/bin/env bash
# Submit a resume ingest job to the running frosty-api container.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${FROSTY_ENV_FILE:-$ROOT/.env}"
LOG_DIR="${FROSTY_LOG_DIR:-$ROOT/logs}"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

PORT="${FROSTY_API_PORT:-8080}"
URL="http://127.0.0.1:${PORT}"
TIMESTAMP="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

mkdir -p "$LOG_DIR"

if ! curl -sf --max-time 10 "${URL}/health" >/dev/null; then
  echo "${TIMESTAMP} ERROR frosty-api not reachable at ${URL}/health"
  exit 1
fi

HEADERS=(-H "Content-Type: application/json")
if [[ -n "${FROSTY_API_KEY:-}" ]]; then
  HEADERS+=(-H "X-API-Key: ${FROSTY_API_KEY}")
fi

RESPONSE="$(curl -sf --max-time 30 -X POST "${URL}/v1/jobs/ingest" \
  "${HEADERS[@]}" \
  -d '{"resume": true}')"

echo "${TIMESTAMP} OK ${RESPONSE}"
