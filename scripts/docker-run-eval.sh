#!/usr/bin/env bash
set -euo pipefail

: "${RUN_DIR:?RUN_DIR must point to runs/<run-id>/}"

cd /mlops-assignment
exec uv run python -m pipeline.docker_entry run-eval --run-dir "${RUN_DIR}"
