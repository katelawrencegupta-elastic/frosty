# Frosty

Decode Splunk frozen `journal.zst` buckets and bulk-ingest events into Elasticsearch.

Frosty reads Splunk's on-disk frozen bucket layout, decodes the binary journal format in pure Python, classifies events (access logs, syslog, AWS CloudTrail, VPC flow logs, and generic), and ships them to Elasticsearch with ingest pipelines and programmatic or HTTP-based orchestration.

## Features

- **Pure-Python journal decoder** — no Rust extensions or external Splunk tools required
- **Event classification** — detects access logs, syslog, AWS CloudTrail, VPC flow logs, and generic events per bucket
- **Ingest pipelines** — deploys GROK, JSON, and dissect parsers plus per-index router pipelines automatically
- **Parallel ingest** — process multiple buckets concurrently with SQLite checkpointing for resume
- **Three interfaces** — CLI, Python SDK (`FrostyClient`), and FastAPI HTTP service
- **Docker** — containerized API with read-only frozen-data mount and persistent checkpoints
- **Scheduled sync** — hourly cron script deploys pipelines and ingests new buckets via the HTTP API
- **Elastic APM** — optional request and job tracing via the HTTP service

## Requirements

- Python 3.10+
- Elasticsearch 8.x (tested with Elastic Cloud Serverless)
- Splunk frozen buckets on disk (`journal.zst` under `rawdata/`)

## Quick start

### Install

```bash
git clone <repo-url> frosty && cd frosty
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

For the HTTP API and APM support:

```bash
pip install -e ".[api]"
```

### Ingest

```bash
export ELASTIC_API_KEY="your-api-key"

# Preview decode counts (no network calls)
frosty-ingest --dry-run

# Ingest all buckets
frosty-ingest

# Ingest one Splunk index
frosty-ingest --index apache
```

### Deploy pipelines

Pipelines are deployed automatically during ingest (`frosty-ingest` and `POST /v1/jobs/ingest` scan journals, create parser pipelines, and set each index's default pipeline before bulk indexing). You can also manage them directly:

```bash
# Preview detected event kinds per index (no Elasticsearch calls)
frosty-setup-pipelines --scan-only

# Deploy or refresh pipelines for all indices
frosty-setup-pipelines

# Re-run existing documents through the router pipeline
frosty-setup-pipelines --reindex
```

Use `frosty-setup-pipelines` when you need to refresh pipelines without ingesting, or `--reindex` to parse documents that were indexed before pipelines existed. The `--reindex` flag runs `_update_by_query` with the router pipeline — it does not delete or recreate indices.

## Splunk frozen bucket layout

Frosty expects the standard Splunk frozen directory structure:

```
frozen/
  apache/                          # Splunk index name
    db_1778817368_1778790392_0/    # bucket: db_{latest}_{earliest}_{seq}
      rawdata/
        journal.zst
  nginx/
  syslog/
  cloud_trail/                     # AWS CloudTrail (aws:cloudtrail)
  vpc_flowlogs/                    # AWS VPC Flow Logs (aws:cloudwatchlogs:vpcflow)
```

Point frosty at the root `frozen/` directory. Each index subdirectory contains one or more bucket folders with a `rawdata/journal.zst` file.

## CLI reference

### `frosty-ingest`

Decode journals and bulk-index into Elasticsearch (`frosty-{index}`).

| Flag | Default | Description |
|------|---------|-------------|
| `--frozen-dir` | `$FROSTY_FROZEN_DIR` or `/Users/klg/Desktop/frozen` | Root folder with index subdirectories |
| `--elastic-url` | `$ELASTIC_URL` | Elasticsearch endpoint |
| `--api-key` | `$ELASTIC_API_KEY` | API key for authentication |
| `--index` | all | Filter to one Splunk index (repeatable) |
| `--bucket` | all | Filter to one bucket directory name (repeatable) |
| `--batch-size` | `500` | Bulk API batch size |
| `--workers` | `1` | Parallel bucket ingest workers |
| `--checkpoint` | `<frozen-dir>/.frosty-checkpoint.db` | Resume state database |
| `--no-resume` | off | Re-ingest buckets even if checkpointed complete |
| `--force` | off | Clear checkpoint and re-ingest all |
| `--dry-run` | off | Decode and count events without sending |
| `--skip-index-create` | off | Skip automatic index creation |

### `frosty-setup-pipelines`

Scan journals, deploy parser pipelines for detected event kinds, and attach router pipelines to indices.

| Flag | Default | Description |
|------|---------|-------------|
| `--frozen-dir` | `$FROSTY_FROZEN_DIR` | Root folder with index subdirectories |
| `--elastic-url` | `$ELASTIC_URL` | Elasticsearch endpoint |
| `--api-key` | `$ELASTIC_API_KEY` | API key for authentication |
| `--index` | all | Filter to one Splunk index (repeatable) |
| `--scan-only` | off | Print detected event kinds without deploying |
| `--write-json` | off | Write pipeline JSON to `pipelines/` |
| `--reindex` | off | Re-run existing documents through the router pipeline (`_update_by_query`) |
| `--skip-default-pipeline` | off | Deploy pipelines but don't set index default |

## Python SDK

```python
from frosty import FrostyClient, FrostyConfig

client = FrostyClient(FrostyConfig(
    frozen_dir="/path/to/frozen",
    api_key="...",
))

# List discovered buckets
buckets = client.list_buckets(indices=["apache"])

# Scan event kinds per index
scan = client.scan()
for profile in scan.indices:
    print(profile.index_name, profile.event_kinds)

# Dry-run decode counts
dry_run = client.dry_run(indices=["apache"])
print(dry_run.total_events)

# Ingest with parallel workers and resume
result = client.ingest(workers=4, resume=True)
print(result.total_indexed, result.skipped)

# Deploy detected pipelines
client.setup_pipelines(reindex=True)

# Decode without Elasticsearch
for doc in client.decode_bucket(buckets[0]):
    print(doc["message"])
```

### Key modules

| Module | Purpose |
|--------|---------|
| `frosty.client` | `FrostyClient` — high-level scan, ingest, pipeline setup |
| `frosty.buckets` | Discover frozen bucket directories |
| `frosty.journal` | Decode journals into Elasticsearch documents |
| `frosty.event_types` | Event classification (`access_log`, `syslog`, `cloud_trail`, `vpc_flow`, `generic`) |
| `frosty.pipelines` | Ingest pipeline definitions |
| `frosty.elastic` | Elasticsearch bulk, index, and pipeline operations |
| `frosty.checkpoint` | SQLite resume state |
| `frosty.api` | FastAPI HTTP service |

## HTTP API

Install API dependencies, set credentials, and start the service:

```bash
pip install -e ".[api]"
export ELASTIC_API_KEY="your-api-key"
frosty-api
```

For APM tracing, also set `ELASTIC_APM_SERVER_URL` and `ELASTIC_APM_API_KEY` — see [Elastic APM](#elastic-apm).

Interactive docs are available at `http://localhost:${FROSTY_API_PORT:-8080}/docs`.

### Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/health` | — | Service health, version, Elastic/APM status |
| `GET` | `/v1/buckets` | optional | List discovered frozen buckets |
| `POST` | `/v1/jobs/scan` | optional | Background event-kind scan |
| `POST` | `/v1/jobs/dry-run` | optional | Background decode/count |
| `POST` | `/v1/jobs/ingest` | optional | Background ingest to Elasticsearch |
| `POST` | `/v1/jobs/pipelines/setup` | optional | Deploy/reindex pipelines |
| `GET` | `/v1/jobs` | optional | List recent jobs |
| `GET` | `/v1/jobs/{job_id}` | optional | Poll job status and result |
| `POST` | `/v1/elastic/verify` | optional | Verify Elasticsearch connectivity |

Set `FROSTY_API_KEY` to require an `X-API-Key` header on protected routes.

Long-running operations return `202 Accepted` with a `job_id`. Poll `GET /v1/jobs/{job_id}` until `status` is `completed` or `failed`.

### Example requests

```bash
PORT=${FROSTY_API_PORT:-8080}

curl "http://localhost:${PORT}/health"

curl -H "X-API-Key: ${FROSTY_API_KEY}" "http://localhost:${PORT}/v1/buckets"

curl -X POST "http://localhost:${PORT}/v1/jobs/ingest" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: ${FROSTY_API_KEY}" \
  -d '{"indices": ["apache"], "workers": 2, "resume": true}'

curl -X POST "http://localhost:${PORT}/v1/jobs/pipelines/setup" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: ${FROSTY_API_KEY}" \
  -d '{"indices": ["cloud_trail"], "set_default": true, "reindex": true}'

curl -H "X-API-Key: ${FROSTY_API_KEY}" "http://localhost:${PORT}/v1/jobs/{job_id}"

curl -X POST -H "X-API-Key: ${FROSTY_API_KEY}" "http://localhost:${PORT}/v1/elastic/verify"
```

## Docker

```bash
cp .env.example .env
# Edit .env — set ELASTIC_API_KEY and ELASTIC_APM_API_KEY (see Elastic APM below)

docker compose up --build -d
curl http://localhost:${FROSTY_API_PORT:-8080}/health
docker compose logs -f
```

The container:

- Mounts frozen buckets read-only at `/data/frozen`
- Persists checkpoint state in a Docker volume at `/data/checkpoint`
- Exposes port **8080** by default (`FROSTY_API_PORT` in `.env` maps host → container)
- Health-checks `GET /health` and runs as a non-root `frosty` user

Ingest is **on-demand** — the API does not watch the frozen directory. New `journal.zst` buckets are visible immediately via `GET /v1/buckets`, but nothing is sent to Elasticsearch until you POST `/v1/jobs/ingest` (or use the CLI). Each ingest job also deploys pipelines for the indices being processed. For hands-off operation, set up the hourly cron job below.

If port 8080 is already in use on the host, set `FROSTY_API_PORT=8099` (or another free port) in `.env` before starting.

### Hourly sync (cron)

`scripts/hourly-ingest.sh` keeps pipelines and ingest in sync with the frozen directory:

Install (merges with your existing crontab — use an absolute path):

```bash
chmod +x scripts/hourly-ingest.sh

REPO=/path/to/frosty   # e.g. /Users/you/frosty
( crontab -l 2>/dev/null | grep -v 'frosty/scripts/hourly-ingest.sh'; \
  echo "0 * * * * ${REPO}/scripts/hourly-ingest.sh >> ${REPO}/logs/hourly-ingest.log 2>&1" \
) | crontab -
```

The script sources `.env` for `FROSTY_API_PORT` and `FROSTY_API_KEY`, verifies `/health`, then:

1. Runs `POST /v1/jobs/pipelines/setup` and waits for completion (deploys parsers and sets index default pipelines)
2. Submits `POST /v1/jobs/ingest` with `resume: true` (skips buckets already in the checkpoint)

Override paths with `FROSTY_ENV_FILE` or `FROSTY_LOG_DIR`. Tune job polling with `FROSTY_JOB_WAIT_SECONDS` (default `600`) and `FROSTY_JOB_POLL_SECONDS` (default `5`).

Verify and monitor:

```bash
./scripts/hourly-ingest.sh
tail -f logs/hourly-ingest.log

# Poll the submitted job (use job_id from the log line)
curl -H "X-API-Key: ${FROSTY_API_KEY}" \
  "http://localhost:${FROSTY_API_PORT:-8080}/v1/jobs?limit=5"
```

Remove the schedule:

```bash
crontab -l | grep -v 'frosty/scripts/hourly-ingest.sh' | crontab -
```

Standalone run:

```bash
docker build -t frosty-api .

docker run --rm -p 8080:8080 \
  -v /path/to/frozen:/data/frozen:ro \
  -e ELASTIC_API_KEY="your-es-api-key" \
  -e ELASTIC_APM_SERVER_URL="https://your-deployment.apm.region.gcp.elastic.cloud" \
  -e ELASTIC_APM_API_KEY="your-apm-agent-api-key" \
  frosty-api
```

## Elastic APM

The HTTP service can send request traces to Elastic APM when configured. APM credentials are **separate** from `ELASTIC_API_KEY` (the Elasticsearch data key used for ingest).

| Credential | Used for | Where to get it |
|------------|----------|-----------------|
| `ELASTIC_API_KEY` | Bulk ingest, pipelines, index management | Elasticsearch / project API keys |
| `ELASTIC_APM_API_KEY` | APM trace intake | Kibana → **Applications** → **Settings** → **Agent keys** |
| `ELASTIC_APM_SECRET_TOKEN` | APM trace intake (alternative) | Elastic Cloud Console → deployment → **APM & Fleet** |

Create an APM agent key with at least the **`event:write`** privilege. The value should be a base64-encoded string (typically starting with characters like `OGta...`), not an `essu_` Cloud management key.

Example `.env` entries:

```bash
ELASTIC_APM_SERVER_URL=https://your-deployment.apm.us-central1.gcp.elastic.cloud
ELASTIC_APM_API_KEY=your-apm-agent-api-key
ELASTIC_APM_SERVICE_NAME=frosty-api
ELASTIC_APM_ENVIRONMENT=production
```

Verify APM is working:

```bash
curl "http://localhost:${FROSTY_API_PORT:-8080}/health"
# expect: "apm_enabled": true

# check container logs — there should be no "HTTP 401: Unauthenticated" errors
docker compose logs -f
```

Traces appear in Kibana under **Observability → APM → Services → frosty-api**.

### APM troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `apm_enabled: false` | Missing or invalid APM credentials | Set `ELASTIC_APM_API_KEY` or `ELASTIC_APM_SECRET_TOKEN` |
| `ELASTIC_APM_API_KEY matches ELASTIC_API_KEY` | Same key used for ES and APM | Create a dedicated APM agent key |
| `HTTP 401: illegal base64` | Wrong key format (e.g. `essu_` Cloud API key) | Use an APM agent key from Kibana **Agent keys** |
| `HTTP 401: Unauthenticated` | Key lacks APM privileges or wrong deployment | Recreate key with `event:write`; confirm `ELASTIC_APM_SERVER_URL` matches your deployment |

Frosty normalizes `essu_`-prefixed keys when possible, but those keys are for the Elastic Cloud REST API and generally will not work for APM intake. Use an APM agent key or secret token instead.

## Configuration

All settings are driven by environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `FROSTY_FROZEN_DIR` | `/Users/klg/Desktop/frozen` | Splunk frozen bucket root |
| `FROSTY_CHECKPOINT_PATH` | `<frozen-dir>/.frosty-checkpoint.db` | Resume checkpoint database |
| `ELASTIC_URL` | Elastic Cloud endpoint | Elasticsearch URL |
| `ELASTIC_API_KEY` | — | Elasticsearch API key |
| `FROSTY_API_HOST` | `0.0.0.0` | HTTP bind address |
| `FROSTY_API_PORT` | `8080` | HTTP listen port |
| `FROSTY_API_KEY` | — | Require `X-API-Key` header when set |
| `FROSTY_JOB_WORKERS` | `2` | Background job thread pool size |
| `ELASTIC_APM_SERVER_URL` | — | APM server URL; enables tracing when auth is also set |
| `ELASTIC_APM_SECRET_TOKEN` | — | APM secret token (Elastic Cloud **APM & Fleet**) |
| `ELASTIC_APM_API_KEY` | — | APM agent key (Kibana **Applications → Agent keys**); not `ELASTIC_API_KEY` |
| `ELASTIC_APM_SERVICE_NAME` | `frosty-api` | Service name in APM |
| `ELASTIC_APM_ENVIRONMENT` | `production` | APM environment tag |

Cron script overrides (optional):

| Variable | Default | Description |
|----------|---------|-------------|
| `FROSTY_ENV_FILE` | `<repo>/.env` | Env file sourced by `scripts/hourly-ingest.sh` |
| `FROSTY_LOG_DIR` | `<repo>/logs` | Directory for `hourly-ingest.log` |
| `FROSTY_JOB_WAIT_SECONDS` | `600` | Max wait for pipeline setup job to finish |
| `FROSTY_JOB_POLL_SECONDS` | `5` | Poll interval when waiting on jobs |

## Elasticsearch documents

Events are indexed into `frosty-{index}` (e.g. `frosty-apache`) with:

| Field | Description |
|-------|-------------|
| `@timestamp` | Event time from the journal (ISO-8601 UTC) |
| `message` | Raw log line (UTF-8 with Latin-1 fallback) |
| `host`, `source`, `sourcetype` | Splunk metadata (prefixes stripped) |
| `event.kind` | Detected type: `access_log`, `syslog`, `cloud_trail`, `vpc_flow`, or `generic` |
| `event.dataset` | Dataset identifier (e.g. `apache.access_log`) |
| `splunk.index` | Source Splunk index name |
| `splunk.bucket_name` | Bucket directory name |
| `splunk.bucket_latest` | Bucket latest epoch |
| `splunk.bucket_earliest` | Bucket earliest epoch |
| `splunk.index_time` | Splunk index time |
| `splunk.pipeline` | Target parser pipeline name |
| `splunk.classify_reason` | Why this event kind was chosen |

## Ingest pipelines

Frosty scans journals to detect event kinds, deploys shared parser pipelines, and attaches a per-index router (`frosty-pipeline-{index}`) as the index default. New documents are parsed on ingest; use `--reindex` to backfill documents indexed before pipelines existed.

| Pipeline | Purpose |
|----------|---------|
| `frosty-parse-access-log` | Apache/Nginx combined log format (GROK) |
| `frosty-parse-syslog` | Syslog, sshd, sudo patterns (GROK) |
| `frosty-parse-cloud-trail` | AWS CloudTrail JSON events |
| `frosty-parse-vpc-flow` | AWS VPC Flow Log version 2 (dissect → `aws.vpcflow.*`) |
| `frosty-parse-generic` | Fallback passthrough |
| `frosty-pipeline-{index}` | Per-index router (routes by `event.kind`) |

### Event classification

| Kind | Typical sourcetypes | Detection |
|------|---------------------|-----------|
| `access_log` | `access_*`, `nginx`, `apache` | Sourcetype, source path, or combined-log message pattern |
| `syslog` | `syslog`, `linux_syslog` | Sourcetype, `/var/log/syslog` source, or RFC3164 message prefix |
| `cloud_trail` | `aws:cloudtrail` | Sourcetype or JSON message with `eventVersion` |
| `vpc_flow` | `aws:cloudwatchlogs:vpcflow` | Sourcetype or space-delimited flow-log message pattern |
| `generic` | anything else | Fallback when no pattern matches |

Run `frosty-setup-pipelines --scan-only` to preview which pipelines would be deployed for your data.

## Journal decoder

Splunk's binary `journal.zst` format is decoded by a vendored pure-Python implementation in `frosty/splunk_journal/`, adapted from:

- [splunk-ddss-extractor](https://github.com/ponquersohn/splunk_ddss_extractor) (MIT)
- [splunker](https://github.com/fionera/splunker) (Apache-2.0)

## Project structure

```
frosty/
  frosty/
    splunk_journal/   # Binary journal decoder
    buckets.py        # Frozen bucket discovery
    journal.py        # Journal → ES document mapping
    event_types.py    # Event classification
    pipelines.py      # Ingest pipeline definitions
    elastic.py        # Elasticsearch client operations
    checkpoint.py     # SQLite resume state
    client.py         # FrostyClient SDK
    ingest.py         # frosty-ingest CLI
    deploy_pipelines.py  # frosty-setup-pipelines CLI
    api/              # FastAPI service + APM
  scripts/
    hourly-ingest.sh  # Cron helper — pipeline setup + resume ingest
  Dockerfile
  docker-compose.yml
  .env.example
  pyproject.toml
```

## License

MIT. See vendored decoder attributions above for third-party licenses.
