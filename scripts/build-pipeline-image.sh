#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

docker build -t "${PIPELINE_IMAGE:-mlops-pipeline:latest}" -f Dockerfile .
