"""Configurable Airflow DAG: run mini-swe-agent batch, evaluate, log to MLflow."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from pipeline.run_steps import (
    build_manifest,
    build_run_config,
    collect_metrics,
    load_run_config,
    log_mlflow_run,
    prepare_run_dir,
    resolve_eval_report_path,
    upload_run_to_object_storage,
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(os.getenv("PIPELINE_PROJECT_ROOT", Path(__file__).resolve().parents[1]))
HOST_PROJECT_ROOT = Path(os.getenv("PIPELINE_HOST_ROOT", PROJECT_ROOT))
CONTAINER_PROJECT_ROOT = os.getenv("PIPELINE_CONTAINER_ROOT", "/mlops-assignment")
PIPELINE_IMAGE = os.getenv("PIPELINE_IMAGE", "mlops-pipeline:latest")
DOCKER_URL = os.getenv("DOCKER_URL", "unix://var/run/docker.sock")


def to_container_path(path: str | Path) -> str:
    path_str = str(path)
    project_root = str(PROJECT_ROOT.resolve())
    if path_str.startswith(project_root):
        return CONTAINER_PROJECT_ROOT + path_str[len(project_root) :]
    return path_str


def docker_run_dir_template(task_id: str = "prepare_run") -> str:
    return f"{{{{ ti.xcom_pull(task_ids='{task_id}')['container_run_dir'] }}}}"


def pipeline_mounts() -> list[Mount]:
    """Bind-mount run dirs and env file without shadowing the image venv."""
    host_root = str(HOST_PROJECT_ROOT.resolve())
    mounts = [
        Mount(source=f"{host_root}/runs", target="/mlops-assignment/runs", type="bind"),
        Mount(source=f"{host_root}/logs", target="/mlops-assignment/logs", type="bind"),
        Mount(source="/var/run/docker.sock", target="/var/run/docker.sock", type="bind"),
    ]
    env_file = HOST_PROJECT_ROOT / ".env"
    if env_file.exists():
        mounts.append(
            Mount(
                source=str(env_file),
                target="/mlops-assignment/.env",
                type="bind",
                read_only=True,
            )
        )
    return mounts


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
        run_dir_str = str(run_dir)
        return {
            **run_config,
            "run_dir": run_dir_str,
            "container_run_dir": to_container_path(run_dir),
            "preds_path": str(run_dir / "run-agent" / "preds.json"),
        }

    run_agent = DockerOperator(
        task_id="run_agent",
        image=PIPELINE_IMAGE,
        api_version="auto",
        auto_remove="force",
        command=["bash", "{{ 'scripts/docker-run-agent.sh' }}"],
        environment={
            "RUN_DIR": docker_run_dir_template(),
            "MSWEA_COST_TRACKING": "ignore_errors",
        },
        mounts=pipeline_mounts(),
        docker_url=DOCKER_URL,
        mount_tmp_dir=False,
        network_mode="bridge",
        execution_timeout=timedelta(hours=6),
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    run_eval = DockerOperator(
        task_id="run_eval",
        image=PIPELINE_IMAGE,
        api_version="auto",
        auto_remove="force",
        command=["bash", "{{ 'scripts/docker-run-eval.sh' }}"],
        environment={
            "RUN_DIR": docker_run_dir_template(),
            "MSWEA_COST_TRACKING": "ignore_errors",
        },
        mounts=pipeline_mounts(),
        docker_url=DOCKER_URL,
        mount_tmp_dir=False,
        network_mode="bridge",
        execution_timeout=timedelta(hours=4),
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    @task
    def collect_metrics_task(**context) -> dict:
        run_config = context["ti"].xcom_pull(task_ids="prepare_run")
        run_dir = Path(run_config["run_dir"])
        eval_report_path = resolve_eval_report_path(run_dir, run_config)
        metrics = collect_metrics(eval_report_path)
        metrics["run_id"] = run_config["run_id"]
        metrics_path = run_dir / "metrics.json"
        metrics_path.write_text(json.dumps(metrics, indent=2) + "\n")
        return {
            **run_config,
            "eval_report_path": str(eval_report_path),
            "metrics": metrics,
        }

    @task(
        execution_timeout=timedelta(minutes=30),
        retries=3,
        retry_delay=timedelta(minutes=2),
    )
    def upload_artifacts(run_state: dict) -> dict:
        run_dir = Path(run_state["run_dir"])
        run_config = {key: run_state[key] for key in run_state if key != "metrics"}
        eval_report_path = Path(run_state["eval_report_path"])

        remote_artifact_uri = upload_run_to_object_storage(run_dir, run_config["run_id"])
        manifest = build_manifest(
            run_dir,
            run_config,
            eval_report_path,
            remote_artifact_uri=remote_artifact_uri,
        )
        manifest_path = run_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

        return {
            **run_state,
            "remote_artifact_uri": remote_artifact_uri,
            "manifest": manifest,
        }

    @task(
        execution_timeout=timedelta(minutes=10),
        retries=3,
        retry_delay=timedelta(minutes=1),
    )
    def log_to_mlflow(run_state: dict) -> dict:
        run_dir = Path(run_state["run_dir"])
        run_config = load_run_config(run_dir)
        metrics = run_state["metrics"]
        remote_artifact_uri = run_state.get("remote_artifact_uri")

        log_mlflow_run(
            run_config,
            metrics,
            str(run_dir),
            remote_artifact_uri=remote_artifact_uri,
        )
        return run_state.get("manifest", {})

    run_config = prepare_run()
    metrics_state = collect_metrics_task()
    uploaded = upload_artifacts(metrics_state)
    logged = log_to_mlflow(uploaded)

    run_config >> run_agent >> run_eval >> metrics_state >> uploaded >> logged


evaluate_agent_dag()
