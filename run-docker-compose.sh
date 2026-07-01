#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export AIRFLOW_UID="${AIRFLOW_UID:-$(id -u)}"
export PIPELINE_HOST_ROOT="${PIPELINE_HOST_ROOT:-${ROOT_DIR}}"

echo "Building pipeline image..."
bash scripts/build-pipeline-image.sh

echo "Initializing Airflow database..."
docker compose up airflow-init

echo "Starting Airflow and MLflow..."
docker compose up -d

echo ""
echo "Airflow UI: http://localhost:8080  (admin / admin)"
echo "MLflow UI:  http://localhost:5000"
