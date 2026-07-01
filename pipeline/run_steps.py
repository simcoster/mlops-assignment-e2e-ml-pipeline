"""Run preparation, agent batch, evaluation, metrics, and artifact helpers."""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(os.getenv("PIPELINE_PROJECT_ROOT", Path(__file__).resolve().parents[1]))
RUNS_ROOT = PROJECT_ROOT / "runs"

SUBSET_TO_DATASET = {
    "verified": "princeton-nlp/SWE-Bench_Verified",
    "lite": "princeton-nlp/SWE-Bench_Lite",
    "full": "princeton-nlp/SWE-Bench",
}

logger = logging.getLogger(__name__)


def load_env_file(path: Path) -> dict[str, str]:
    """Load KEY=VALUE lines from a .env file (no quotes/expansion)."""
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip()
    return env


def subprocess_env() -> dict[str, str]:
    return {
        **os.environ,
        **load_env_file(PROJECT_ROOT / ".env"),
        "MSWEA_COST_TRACKING": "ignore_errors",
    }


def resolve_swebench_config_path() -> Path:
    """Return path to mini-swe-agent SWE-bench benchmark config."""
    candidates = [
        PROJECT_ROOT / "mini-swe-agent/src/minisweagent/config/benchmarks/swebench.yaml",
        PROJECT_ROOT.parent / "mini-swe-agent/src/minisweagent/config/benchmarks/swebench.yaml",
    ]
    venv_site_packages = PROJECT_ROOT / ".venv/lib"
    if venv_site_packages.exists():
        candidates.extend(
            path
            for path in venv_site_packages.glob(
                "python*/site-packages/minisweagent/config/benchmarks/swebench.yaml"
            )
        )

    for path in candidates:
        if path.exists():
            return path

    raise FileNotFoundError(
        "SWE-bench agent config not found. Run `uv sync` in the project or clone mini-swe-agent."
    )


def build_run_config(params: dict) -> dict:
    run_id = (params.get("run_id") or "").strip()
    if not run_id or run_id.lower() == "auto":
        run_id = datetime.now(timezone.utc).strftime("run-%Y%m%d-%H%M%S")

    subset = params["subset"]
    dataset_name = SUBSET_TO_DATASET.get(subset, subset)
    model = params["model"]
    workers = int(params["workers"])
    cost_limit = float(params["cost_limit"])

    return {
        "run_id": run_id,
        "split": params["split"],
        "subset": subset,
        "dataset_name": dataset_name,
        "model": model,
        "task_slice": params["task_slice"],
        "workers": workers,
        "cost_limit": cost_limit,
        "model_sanitized": model.replace("/", "__"),
    }


def load_run_config(run_dir: Path) -> dict:
    config_path = run_dir / "config.json"
    if not config_path.exists():
        raise FileNotFoundError(f"Missing run config at {config_path}")
    return json.loads(config_path.read_text())


def prepare_run_dir(run_config: dict) -> Path:
    run_dir = RUNS_ROOT / run_config["run_id"]
    agent_dir = run_dir / "run-agent"
    trajectories_dir = agent_dir / "trajectories"
    eval_dir = run_dir / "run-eval"
    logs_dir = eval_dir / "logs"
    reports_dir = eval_dir / "reports"

    for path in (agent_dir, trajectories_dir, logs_dir, reports_dir):
        path.mkdir(parents=True, exist_ok=True)

    config_path = run_dir / "config.json"
    config_path.write_text(json.dumps(run_config, indent=2) + "\n")
    return run_dir


def organize_agent_artifacts(agent_dir: Path) -> Path:
    """Move per-instance outputs into run-agent/trajectories/."""
    trajectories_dir = agent_dir / "trajectories"
    trajectories_dir.mkdir(parents=True, exist_ok=True)

    for path in list(agent_dir.iterdir()):
        if path.name in {"preds.json", "trajectories"}:
            continue
        dest = trajectories_dir / path.name
        if dest.exists():
            if dest.is_dir():
                shutil.rmtree(dest)
            else:
                dest.unlink()
        shutil.move(str(path), str(dest))

    return trajectories_dir


def run_agent_batch(run_config: dict, run_dir: Path) -> Path:
    agent_dir = run_dir / "run-agent"
    cmd = [
        "uv",
        "run",
        "mini-extra",
        "swebench",
        "--subset",
        run_config["subset"],
        "--split",
        run_config["split"],
        "--model",
        run_config["model"],
        "--slice",
        run_config["task_slice"],
        "--workers",
        str(run_config["workers"]),
        "--config",
        str(resolve_swebench_config_path()),
        "-o",
        str(agent_dir),
        "-c",
        f"agent.cost_limit={run_config['cost_limit']}",
    ]
    subprocess.run(cmd, cwd=PROJECT_ROOT, env=subprocess_env(), check=True)
    preds_path = agent_dir / "preds.json"
    if not preds_path.exists():
        raise FileNotFoundError(f"Expected predictions file at {preds_path}")
    organize_agent_artifacts(agent_dir)
    return preds_path


def run_swebench_eval(run_config: dict, preds_path: Path, run_dir: Path) -> Path:
    eval_dir = run_dir / "run-eval"
    reports_dir = eval_dir / "reports"
    run_id = run_config["run_id"]

    cmd = [
        "uv",
        "run",
        "python",
        "-m",
        "swebench.harness.run_evaluation",
        "--dataset_name",
        run_config["dataset_name"],
        "--split",
        run_config["split"],
        "--predictions_path",
        str(preds_path),
        "--max_workers",
        str(run_config["workers"]),
        "--run_id",
        run_id,
        "--report_dir",
        str(reports_dir),
    ]
    subprocess.run(cmd, cwd=PROJECT_ROOT, env=subprocess_env(), check=True)

    harness_logs = PROJECT_ROOT / "logs" / "run_evaluation" / run_id
    if harness_logs.exists():
        dest = eval_dir / "logs" / "run_evaluation" / run_id
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            shutil.rmtree(dest)
        shutil.move(str(harness_logs), str(dest))

    aggregate_name = f"{run_config['model_sanitized']}.{run_id}.json"
    aggregate_in_root = PROJECT_ROOT / aggregate_name
    aggregate_in_reports = reports_dir / aggregate_name
    if aggregate_in_root.exists():
        shutil.move(str(aggregate_in_root), str(aggregate_in_reports))
    elif not aggregate_in_reports.exists():
        candidates = list(reports_dir.glob(f"*.{run_id}.json"))
        if not candidates:
            raise FileNotFoundError(
                f"Expected aggregate eval report for run_id={run_id} in {reports_dir}"
            )
        aggregate_in_reports = candidates[0]

    return aggregate_in_reports


def numeric_metrics_for_mlflow(metrics: dict) -> dict:
    """MLflow metrics must be numeric; exclude string fields like run_id."""
    return {
        key: float(value)
        for key, value in metrics.items()
        if isinstance(value, (int, float)) and not isinstance(value, bool)
    }


def collect_metrics(eval_report_path: Path) -> dict:
    report = json.loads(eval_report_path.read_text())
    submitted = report.get("submitted_instances", 0)
    resolved = report.get("resolved_instances", 0)
    completed = report.get("completed_instances", 0)
    total = report.get("total_instances", 0)
    resolve_rate = (resolved / submitted) if submitted else 0.0

    return {
        "total_instances": total,
        "submitted_instances": submitted,
        "completed_instances": completed,
        "resolved_instances": resolved,
        "unresolved_instances": report.get("unresolved_instances", 0),
        "empty_patch_instances": report.get("empty_patch_instances", 0),
        "error_instances": report.get("error_instances", 0),
        "resolve_rate": resolve_rate,
    }


def build_manifest(
    run_dir: Path,
    run_config: dict,
    eval_report_path: Path,
    remote_artifact_uri: str | None = None,
) -> dict:
    agent_dir = run_dir / "run-agent"
    trajectories_dir = agent_dir / "trajectories"
    agent_log = trajectories_dir / "minisweagent.log"

    manifest = {
        "run_id": run_config["run_id"],
        "artifact_root": str(run_dir.resolve()),
        "config": str((run_dir / "config.json").resolve()),
        "preds": str((agent_dir / "preds.json").resolve()),
        "trajectories": str(trajectories_dir.resolve()),
        "trajectory_files": sorted(str(path.resolve()) for path in trajectories_dir.rglob("*.traj.json")),
        "eval_report": str(eval_report_path.resolve()),
        "eval_logs": str((run_dir / "run-eval" / "logs").resolve()),
        "eval_reports": str((run_dir / "run-eval" / "reports").resolve()),
        "metrics": str((run_dir / "metrics.json").resolve()),
    }
    if agent_log.exists():
        manifest["agent_log"] = str(agent_log.resolve())
    if remote_artifact_uri:
        manifest["remote_artifact_uri"] = remote_artifact_uri
    return manifest


def _object_storage_settings() -> dict[str, str]:
    file_env = load_env_file(PROJECT_ROOT / ".env")
    return {
        key: os.getenv(key, file_env.get(key, "")).strip()
        for key in (
            "ARTIFACTS_S3_URI",
            "S3_BUCKET",
            "S3_PREFIX",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_DEFAULT_REGION",
            "AWS_ENDPOINT_URL",
        )
    }


def create_run_archive(run_dir: Path) -> Path:
    archive_path = run_dir.with_suffix(".tar.gz")
    if archive_path.exists():
        archive_path.unlink()
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(run_dir, arcname=run_dir.name)
    return archive_path


def upload_run_to_object_storage(run_dir: Path, run_id: str) -> str | None:
    settings = _object_storage_settings()
    bucket = settings["S3_BUCKET"]
    prefix = settings["S3_PREFIX"].strip("/") or "runs"
    configured_uri = settings["ARTIFACTS_S3_URI"]

    if configured_uri:
        parsed = urlparse(configured_uri)
        if parsed.scheme != "s3" or not parsed.netloc:
            raise ValueError(f"ARTIFACTS_S3_URI must look like s3://bucket/prefix, got {configured_uri!r}")
        bucket = parsed.netloc
        prefix = parsed.path.strip("/")

    if not bucket:
        return None

    import boto3
    from botocore.config import Config

    archive_path = create_run_archive(run_dir)
    key = "/".join(part for part in (prefix, f"{run_id}.tar.gz") if part)

    client_kwargs: dict = {}
    if settings["AWS_ENDPOINT_URL"]:
        client_kwargs["endpoint_url"] = settings["AWS_ENDPOINT_URL"]
    if settings["AWS_DEFAULT_REGION"]:
        client_kwargs["region_name"] = settings["AWS_DEFAULT_REGION"]

    client = boto3.client("s3", config=Config(signature_version="s3v4"), **client_kwargs)
    client.upload_file(str(archive_path), bucket, key)
    archive_path.unlink(missing_ok=True)
    return f"s3://{bucket}/{key}"


def log_mlflow_run(
    run_config: dict,
    metrics: dict,
    artifact_uri: str,
    remote_artifact_uri: str | None = None,
) -> None:
    import mlflow

    tracking_uri = os.getenv(
        "MLFLOW_TRACKING_URI",
        load_env_file(PROJECT_ROOT / ".env").get("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000"),
    )
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("evaluate_agent")

    with mlflow.start_run(run_name=run_config["run_id"]):
        mlflow.log_params(
            {
                "run_id": run_config["run_id"],
                "split": str(run_config["split"]),
                "subset": str(run_config["subset"]),
                "model": str(run_config["model"]),
                "task_slice": str(run_config["task_slice"]),
                "workers": str(run_config["workers"]),
                "cost_limit": str(run_config["cost_limit"]),
            }
        )
        mlflow.log_metrics(numeric_metrics_for_mlflow(metrics))
        mlflow.set_tag("artifact_path", artifact_uri)
        if remote_artifact_uri:
            mlflow.log_param("remote_artifact_uri", remote_artifact_uri)
            mlflow.set_tag("remote_artifact_uri", remote_artifact_uri)

        metrics_file = Path(artifact_uri) / "metrics.json"
        manifest_file = Path(artifact_uri) / "manifest.json"
        if metrics_file.exists():
            mlflow.log_artifact(str(metrics_file))
        if manifest_file.exists():
            mlflow.log_artifact(str(manifest_file))


def resolve_eval_report_path(run_dir: Path, run_config: dict) -> Path:
    reports_dir = run_dir / "run-eval" / "reports"
    aggregate_name = f"{run_config['model_sanitized']}.{run_config['run_id']}.json"
    aggregate_path = reports_dir / aggregate_name
    if aggregate_path.exists():
        return aggregate_path

    candidates = list(reports_dir.glob(f"*.{run_config['run_id']}.json"))
    if not candidates:
        raise FileNotFoundError(
            f"Expected aggregate eval report for run_id={run_config['run_id']} in {reports_dir}"
        )
    return candidates[0]
