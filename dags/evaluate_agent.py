"""Configurable Airflow DAG: run mini-swe-agent batch, evaluate, log to MLflow."""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param

PROJECT_ROOT = Path(__file__).resolve().parents[1]
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


def prepare_run_dir(run_config: dict) -> Path:
    run_dir = RUNS_ROOT / run_config["run_id"]
    agent_dir = run_dir / "run-agent"
    eval_dir = run_dir / "run-eval"
    logs_dir = eval_dir / "logs"
    reports_dir = eval_dir / "reports"

    for path in (agent_dir, logs_dir, reports_dir):
        path.mkdir(parents=True, exist_ok=True)

    config_path = run_dir / "config.json"
    config_path.write_text(json.dumps(run_config, indent=2) + "\n")
    return run_dir


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


def log_mlflow_run(run_config: dict, metrics: dict, artifact_uri: str) -> None:
    tracking_uri = os.getenv(
        "MLFLOW_TRACKING_URI",
        load_env_file(PROJECT_ROOT / ".env").get("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000"),
    )
    payload = {
        "tracking_uri": tracking_uri,
        "run_config": run_config,
        "metrics": numeric_metrics_for_mlflow(metrics),
        "artifact_uri": artifact_uri,
    }
    script = """
import json, os, sys
import mlflow

payload = json.loads(os.environ["MLFLOW_PAYLOAD"])
run_config = payload["run_config"]
metrics = payload["metrics"]
artifact_uri = payload["artifact_uri"]

mlflow.set_tracking_uri(payload["tracking_uri"])
mlflow.set_experiment("evaluate_agent")

with mlflow.start_run(run_name=run_config["run_id"]):
    mlflow.log_params({
        "run_id": run_config["run_id"],
        "split": str(run_config["split"]),
        "subset": str(run_config["subset"]),
        "model": str(run_config["model"]),
        "task_slice": str(run_config["task_slice"]),
        "workers": str(run_config["workers"]),
        "cost_limit": str(run_config["cost_limit"]),
    })
    mlflow.log_metrics(metrics)
    mlflow.set_tag("artifact_path", artifact_uri)
    metrics_file = os.path.join(artifact_uri, "metrics.json")
    if os.path.exists(metrics_file):
        mlflow.log_artifact(metrics_file)
"""
    env = subprocess_env()
    env["MLFLOW_PAYLOAD"] = json.dumps(payload)
    subprocess.run(
        ["uv", "run", "python", "-c", script],
        cwd=PROJECT_ROOT,
        env=env,
        check=True,
    )


@dag(
    dag_id="evaluate_agent",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "split": Param("test", type="string", description="SWE-bench dataset split"),
        "subset": Param("verified", type="string", description="SWE-bench subset name"),
        "workers": Param(3, type="integer", description="Parallel workers for agent and eval"),
        "model": Param(
            "nebius/moonshotai/Kimi-K2.6",
            type="string",
            description="Model passed to mini-swe-agent",
        ),
        "task_slice": Param("0:3", type="string", description="Instance slice, e.g. 0:3"),
        "run_id": Param(
            "auto",
            type="string",
            description="Run ID ('auto' or empty = auto-generated timestamp)",
        ),
        "cost_limit": Param(0, type="number", description="Agent cost limit (0 = disabled)"),
    },
)
def evaluate_agent_dag():
    @task
    def prepare_run(**context) -> dict:
        run_config = build_run_config(context["params"])
        run_dir = prepare_run_dir(run_config)
        return {
            **run_config,
            "run_dir": str(run_dir),
            "preds_path": str(run_dir / "run-agent" / "preds.json"),
        }

    @task
    def run_agent(run_config: dict) -> dict:
        run_dir = Path(run_config["run_dir"])
        preds_path = run_agent_batch(run_config, run_dir)
        return {**run_config, "preds_path": str(preds_path)}

    @task
    def run_eval(run_config: dict) -> dict:
        run_dir = Path(run_config["run_dir"])
        preds_path = Path(run_config["preds_path"])
        eval_report_path = run_swebench_eval(run_config, preds_path, run_dir)
        return {**run_config, "eval_report_path": str(eval_report_path)}

    @task
    def summarize_and_log(run_config: dict) -> dict:
        run_dir = Path(run_config["run_dir"])
        eval_report_path = Path(run_config["eval_report_path"])

        metrics = collect_metrics(eval_report_path)
        metrics["run_id"] = run_config["run_id"]
        metrics_path = run_dir / "metrics.json"
        metrics_path.write_text(json.dumps(metrics, indent=2) + "\n")

        manifest = {
            "run_id": run_config["run_id"],
            "artifact_root": str(run_dir),
            "config": str(run_dir / "config.json"),
            "preds": run_config["preds_path"],
            "eval_report": str(eval_report_path),
            "eval_logs": str(run_dir / "run-eval" / "logs"),
            "metrics": str(metrics_path),
        }
        manifest_path = run_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

        try:
            log_mlflow_run(run_config, metrics, str(run_dir))
        except Exception:
            logger.exception("MLflow logging failed; run artifacts are still on disk at %s", run_dir)

        return manifest

    run_config = prepare_run()
    agent_result = run_agent(run_config)
    eval_result = run_eval(agent_result)
    summarize_and_log(eval_result)


evaluate_agent_dag()
