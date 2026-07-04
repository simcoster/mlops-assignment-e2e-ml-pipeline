# Evaluation Pipeline Report

## Overview

This project turns ad-hoc mini-swe-agent + SWE-bench scripts into a configurable Airflow pipeline with durable artifacts, MLflow tracking, and S3-compatible object storage (MinIO).

The main DAG is `evaluate_agent` (`dags/evaluate_agent.py`). It implements:

```text
prepare_run → run_agent → run_eval → collect_metrics_task → upload_artifacts → log_to_mlflow
```

- **`prepare_run`**: reads Airflow params, creates `runs/<run-id>/config.json`
- **`run_agent`**: `DockerOperator` running `mini-extra swebench` in `mlops-pipeline:latest`
- **`run_eval`**: `DockerOperator` running SWE-bench harness evaluation (Docker-in-Docker via mounted socket)
- **`collect_metrics_task`**: parses eval report → `metrics.json`
- **`upload_artifacts`**: tars the run folder and uploads to MinIO
- **`log_to_mlflow`**: logs params, metrics, and artifact references to MLflow

Pipeline logic lives in `pipeline/run_steps.py`. Agent/eval steps run inside the project `Dockerfile` image via `scripts/docker-run-agent.sh` and `scripts/docker-run-eval.sh`.

## Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│  Docker Compose (production-style)                          │
│                                                             │
│  Airflow (scheduler + webserver)                            │
│    └─ evaluate_agent DAG                                    │
│         ├─ DockerOperator → mlops-pipeline:latest (agent)   │
│         ├─ DockerOperator → mlops-pipeline:latest (eval)    │
│         ├─ upload_artifacts → MinIO (S3 API)              │
│         └─ log_to_mlflow → MLflow                           │
│                                                             │
│  MLflow tracking server                                     │
│  MinIO object storage (bucket: mlops-runs)                  │
│  Postgres (Airflow metadata)                                │
└─────────────────────────────────────────────────────────────┘
```

Bind mounts from the host repo into Airflow and pipeline containers:

- `runs/` — per-run artifacts
- `logs/` — Airflow and SWE-bench logs
- `.env` — API keys and storage config
- `/var/run/docker.sock` — required for `DockerOperator` and SWE-bench eval containers

## Deployment

### Prerequisites

- Docker + Docker Compose
- `NEBIUS_API_KEY` in `.env` (copy from `.env.example`)
- `PIPELINE_HOST_ROOT` set to the absolute path of this repo in `.env`

### Start the stack

```bash
cp .env.example .env   # if not done already
# edit .env: NEBIUS_API_KEY, PIPELINE_HOST_ROOT

bash run-docker-compose.sh
```

Or manually:

```bash
bash scripts/build-pipeline-image.sh
docker compose up airflow-init
docker compose up -d --wait
```

### Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| MLflow | http://localhost:5000 | — |
| MinIO API | http://localhost:9000 | `minioadmin` / `minioadmin` |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |

Screenshots: `screenshots/airflow_dag.png`, `screenshots/mlflow_runs.png`, `screenshots/object_storage_artifacts.png`.

## Triggering a run

### Airflow UI

1. Open http://localhost:8080
2. Unpause DAG `evaluate_agent` if needed
3. Click **Trigger DAG w/ config** and set params, for example:

```json
{
  "split": "test",
  "subset": "verified",
  "workers": 1,
  "model": "nebius/moonshotai/Kimi-K2.6",
  "task_slice": "0:1",
  "run_id": "auto",
  "cost_limit": 1.0
}
```

### CLI

```bash
docker compose exec airflow-scheduler airflow dags trigger evaluate_agent \
  --conf '{"split":"test","subset":"verified","workers":1,"task_slice":"0:1","run_id":"auto","cost_limit":1.0}'
```

### Parameters

| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| `split` | yes | `test` | SWE-bench split |
| `subset` | yes | `verified` | SWE-bench subset |
| `workers` | yes | `1` | Parallel workers for agent + eval |
| `model` | no | `nebius/moonshotai/Kimi-K2.6` | LLM for mini-swe-agent |
| `task_slice` | no | `0:1` | Instance slice, e.g. `0:1` or `4:5` |
| `run_id` | no | `auto` | Run folder name; `auto` → timestamped ID |
| `cost_limit` | no | `1.0` | Max agent spend in USD per run (must be > 0) |

`workers` controls parallelism **inside** the agent/eval containers (not Airflow task parallelism). Use `workers` ≤ number of instances in `task_slice`.

### Cost limits

Agent spend is capped in three ways:

1. **`agent.cost_limit`** — per-instance dollar budget passed to mini-swe-agent
2. **`MSWEA_GLOBAL_COST_LIMIT`** — process-wide dollar cap (same value as `cost_limit`)
3. **`MSWEA_GLOBAL_CALL_LIMIT` / `agent.step_limit`** — hard cap on API calls (~20 calls per $1)

Nebius model pricing is registered in `config/litellm_model_registry.json` so LiteLLM can track real token costs. Values of `cost_limit <= 0` are rejected and fall back to `AGENT_COST_LIMIT_DEFAULT` in `.env` (default `$1.00`).

For a cheap smoke run, use `task_slice: "0:1"`, `workers: 1`, and `cost_limit: 0.5`–`1.0`.

## Artifact layout

Each successful run writes:

```text
runs/<run-id>/
  config.json              # resolved experiment config
  run-agent/
    preds.json             # predictions for SWE-bench harness
    trajectories/          # per-instance agent outputs + minisweagent.log
  run-eval/
    logs/                  # harness logs (copied from logs/run_evaluation/)
    reports/               # per-run aggregate eval report JSON
  metrics.json             # parsed resolve rate, instance counts, etc.
  manifest.json            # index of all important paths + remote URI
```

`manifest.json` is the entry point for reconstructing a run. Example from completed run `run-20260704-112922`:

```json
{
  "run_id": "run-20260704-112922",
  "artifact_root": ".../runs/run-20260704-112922",
  "config": ".../config.json",
  "preds": ".../run-agent/preds.json",
  "trajectories": ".../run-agent/trajectories",
  "trajectory_files": [".../astropy__astropy-13453.traj.json"],
  "eval_report": ".../run-eval/reports/nebius__moonshotai__Kimi-K2.6.run-20260704-112922.json",
  "eval_logs": ".../run-eval/logs",
  "metrics": ".../metrics.json",
  "remote_artifact_uri": "s3://mlops-runs/runs/run-20260704-112922.tar.gz"
}
```

Full run directories are gitignored (`runs/`). A committed example of run metadata lives in `sample/run_manifest/` (`config.json`, `metrics.json`, `manifest.json`). A tarball for each run is uploaded to MinIO when `S3_BUCKET` is configured.

## Completed evaluation example

**Run ID:** `run-20260704-112922` (documented in all three screenshots)

| Field | Value |
|-------|-------|
| Config | `split=test`, `subset=verified`, `task_slice=4:5`, `workers=1` |
| Model | `nebius/moonshotai/Kimi-K2.6` |
| Instance | `astropy__astropy-13453` |
| Resolved | 1 / 1 |
| Resolve rate | 1.0 |
| Cost limit | $0.50 |

The pipeline completed end-to-end: the agent produced a patch, SWE-bench evaluation passed, metrics were written, artifacts were uploaded to MinIO, and the run was logged in MLflow. The Airflow grid view shows earlier failed attempts from debugging; the rightmost column is the successful run.

**Remote artifact URI:**

```text
s3://mlops-runs/runs/run-20260704-112922.tar.gz
```

Download from MinIO Console (http://localhost:9001) → bucket `mlops-runs` → prefix `runs/`, or with the AWS CLI pointed at `http://127.0.0.1:9000`.

## MLflow tracking

- **Experiment:** `evaluate_agent`
- **UI:** http://localhost:5000
- **Logged per run:** params (`split`, `subset`, `model`, `task_slice`, `workers`, `cost_limit`, `call_limit`, `step_limit`, `run_id`), metrics (`resolve_rate`, `resolved_instances`, etc.), tags (`artifact_path`, `remote_artifact_uri`)

From inside Docker Compose, Airflow uses `MLFLOW_TRACKING_URI=http://mlflow:5000`. From the host browser, use http://localhost:5000.

See `screenshots/mlflow_runs.png`.

## Object storage (MinIO)

MinIO provides a local S3-compatible endpoint. Configuration in `.env`:

```bash
S3_BUCKET=mlops-runs
S3_PREFIX=runs
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_ENDPOINT_URL=http://minio:9000   # inside compose; use http://127.0.0.1:9000 from host
```

`upload_artifacts` creates `runs/<run-id>.tar.gz` and uploads to `s3://mlops-runs/runs/<run-id>.tar.gz`. The URI is stored in `manifest.json` and logged to MLflow.

See `screenshots/object_storage_artifacts.png`.

## Rerunning / inspecting a run

### Rerun with a new config

Trigger the DAG again with different params. A new `runs/<run-id>/` folder is created (auto ID unless `run_id` is set explicitly).

### Reconstruct an existing run locally

If you have the local folder:

```bash
ls runs/run-20260704-112922/
cat runs/run-20260704-112922/manifest.json
cat runs/run-20260704-112922/metrics.json
```

If you only have the remote tarball:

1. Download `s3://mlops-runs/runs/<run-id>.tar.gz` from MinIO
2. `tar -xzf <run-id>.tar.gz`
3. Use `manifest.json` inside the extracted folder to navigate preds, trajectories, eval logs, and reports

### Re-run evaluation only (manual)

Given an existing `preds.json`:

```bash
export RUN_DIR=runs/<run-id>
bash scripts/docker-run-eval.sh   # inside pipeline container, or via equivalent uv command
```

## Retries and timeouts

| Task | Timeout | Retries |
|------|---------|---------|
| `run_agent` | 6 h | 1 |
| `run_eval` | 4 h | 1 |
| `upload_artifacts` | 30 min | 3 |
| `log_to_mlflow` | 10 min | 3 |

## Files of note

| Path | Role |
|------|------|
| `dags/evaluate_agent.py` | Main DAG |
| `pipeline/run_steps.py` | Run helpers (config, agent, eval, metrics, upload, MLflow) |
| `config/litellm_model_registry.json` | Nebius model pricing for cost tracking |
| `Dockerfile` | Pipeline execution image |
| `docker-compose.yaml` | Airflow + MLflow + MinIO + Postgres |
| `run-docker-compose.sh` | One-command production startup |
| `.env.example` | Environment template |
