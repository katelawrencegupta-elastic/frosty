#!/usr/bin/env bash
# Deploy ingest pipelines, then submit a resume ingest job to frosty-api.
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
JOB_WAIT_SECONDS="${FROSTY_JOB_WAIT_SECONDS:-600}"
JOB_POLL_SECONDS="${FROSTY_JOB_POLL_SECONDS:-5}"

mkdir -p "$LOG_DIR"

HEADERS=(-H "Content-Type: application/json")
if [[ -n "${FROSTY_API_KEY:-}" ]]; then
  HEADERS+=(-H "X-API-Key: ${FROSTY_API_KEY}")
fi

if ! curl -sf --max-time 10 "${URL}/health" >/dev/null; then
  echo "${TIMESTAMP} ERROR frosty-api not reachable at ${URL}/health"
  exit 1
fi

submit_job() {
  local path="$1"
  local body="${2:-{}}"
  curl -sf --max-time 30 -X POST "${URL}${path}" "${HEADERS[@]}" -d "${body}"
}

wait_for_job() {
  local job_id="$1"
  local deadline=$(( $(date +%s) + JOB_WAIT_SECONDS ))
  while [[ "$(date +%s)" -lt "$deadline" ]]; do
    local status
    status="$(curl -sf --max-time 15 "${URL}/v1/jobs/${job_id}" "${HEADERS[@]}" \
      | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")"
    case "${status}" in
      completed) return 0 ;;
      failed)
        curl -sf "${URL}/v1/jobs/${job_id}" "${HEADERS[@]}" || true
        return 1
        ;;
    esac
    sleep "${JOB_POLL_SECONDS}"
  done
  echo "timed out waiting for job ${job_id}" >&2
  return 1
}

PIPELINE_JOB="$(submit_job "/v1/jobs/pipelines/setup" '{"set_default": true, "reindex": false}')"
PIPELINE_ID="$(printf '%s' "${PIPELINE_JOB}" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])")"
if ! wait_for_job "${PIPELINE_ID}"; then
  echo "${TIMESTAMP} ERROR pipeline setup job failed: ${PIPELINE_JOB}"
  exit 1
fi
echo "${TIMESTAMP} OK pipelines ${PIPELINE_JOB}"

INGEST_JOB="$(submit_job "/v1/jobs/ingest" '{"resume": true}')"
echo "${TIMESTAMP} OK ingest ${INGEST_JOB}"
