#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export AIRFLOW_UID="${AIRFLOW_UID:-50000}"
export DOCKER_GID="${DOCKER_GID:-$(getent group docker | cut -d: -f3)}"
export PIPELINE_HOST_ROOT="${PIPELINE_HOST_ROOT:-${ROOT_DIR}}"

mkdir -p runs logs/airflow
chmod -R a+rwX runs logs 2>/dev/null || true

echo "Building pipeline image..."
bash scripts/build-pipeline-image.sh

echo "Initializing Airflow database..."
docker compose up airflow-init

echo "Starting Airflow, MLflow, and MinIO (waiting until healthy)..."
docker compose up -d --wait

echo ""
echo "Services are ready:"
echo "Airflow UI: http://localhost:8080  (admin / admin)"
echo "MLflow UI:  http://localhost:5000"
echo "MinIO API:  http://localhost:9000  (minioadmin / minioadmin)"
echo "MinIO UI:   http://localhost:9001"
