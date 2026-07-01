"""CLI entry points for pipeline steps executed inside Docker containers."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.run_steps import (
    load_run_config,
    run_agent_batch,
    run_swebench_eval,
)


def _run_agent(run_dir: Path) -> None:
    run_config = load_run_config(run_dir)
    run_agent_batch(run_config, run_dir)


def _run_eval(run_dir: Path) -> None:
    run_config = load_run_config(run_dir)
    preds_path = run_dir / "run-agent" / "preds.json"
    if not preds_path.exists():
        raise FileNotFoundError(f"Missing predictions at {preds_path}")
    run_swebench_eval(run_config, preds_path, run_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run isolated pipeline steps.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    agent_parser = subparsers.add_parser("run-agent", help="Run mini-swe-agent batch.")
    agent_parser.add_argument("--run-dir", type=Path, required=True)

    eval_parser = subparsers.add_parser("run-eval", help="Run SWE-bench evaluation.")
    eval_parser.add_argument("--run-dir", type=Path, required=True)

    args = parser.parse_args()
    if args.command == "run-agent":
        _run_agent(args.run_dir)
    elif args.command == "run-eval":
        _run_eval(args.run_dir)


if __name__ == "__main__":
    main()
