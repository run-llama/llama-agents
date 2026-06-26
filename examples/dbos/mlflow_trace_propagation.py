# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

"""DBOS + MLflow trace propagation example.

Run with:
    uv run --package llama-agents-dbos --with mlflow --with llama-index \
        examples/dbos/mlflow_trace_propagation.py --check
"""

from __future__ import annotations

import argparse
import asyncio
import shutil
import uuid
from pathlib import Path

import mlflow
import mlflow.llama_index
from dbos import DBOS
from llama_agents.dbos import DBOSRuntime
from pydantic import Field
from workflows import Context, Workflow, step
from workflows.events import Event, StartEvent, StopEvent
from workflows.handler import WorkflowHandler

_DIR = Path(__file__).parent
_DB_FILE = _DIR / ".dbos_mlflow.sqlite3"
_MLFLOW_DB_FILE = _DIR / ".mlflow_tracking.sqlite3"
_MLRUNS_DIR = _DIR / "mlruns"
_EXPERIMENT_NAME = "dbos-mlflow-propagation"


class Work(Event):
    value: str = Field(description="Value to process")


class WorkStart(StartEvent):
    value: str = Field(description="Value to process")


class WorkResult(StopEvent):
    final_result: str = Field(description="Processed value")


class TracePropagationWorkflow(Workflow):
    @step
    async def start(self, ctx: Context, ev: WorkStart) -> Work:
        return Work(value=ev.value)

    @step
    async def process(self, ctx: Context, ev: Work) -> WorkResult:
        result = ev.value.upper()
        with mlflow.start_span("manual-dbospan") as span:
            span.set_inputs({"value": ev.value})
            span.set_outputs({"result": result})
        return WorkResult(final_result=result)


async def _run_workflow(runtime: DBOSRuntime, run_id: str) -> WorkResult:
    workflow = TracePropagationWorkflow(runtime=runtime, timeout=10)
    handler = workflow.run(start_event=WorkStart(value="dbos"), run_id=run_id)
    if not isinstance(handler, WorkflowHandler):
        raise TypeError(f"Expected WorkflowHandler, got {type(handler)!r}")
    result = await handler
    if not isinstance(result, WorkResult):
        raise TypeError(f"Expected WorkResult, got {type(result)!r}")
    return result


def _configure_mlflow() -> str:
    mlflow.set_tracking_uri(f"sqlite:///{_MLFLOW_DB_FILE}")
    experiment = mlflow.set_experiment(_EXPERIMENT_NAME)
    mlflow.llama_index.autolog()
    return experiment.experiment_id


def _find_trace(experiment_id: str, trace_id: str):
    traces = mlflow.search_traces(
        locations=[experiment_id],
        return_type="list",
        include_spans=True,
        flush=True,
    )
    return next(trace for trace in traces if trace.info.trace_id == trace_id)


def _check_trace(experiment_id: str, trace_id: str, root_span_id: str) -> None:
    trace = _find_trace(experiment_id, trace_id)
    spans = trace.data.spans
    manual_span = next(span for span in spans if span.name == "manual-dbospan")
    manual_parent = next(
        span for span in spans if span.span_id == manual_span.parent_id
    )

    assert any(span.parent_id == root_span_id for span in spans)
    assert manual_parent.trace_id == trace_id
    assert manual_span.trace_id == trace_id
    assert manual_span.parent_id != root_span_id

    print(f"trace_id={trace_id}")
    print(f"root_span_id={root_span_id}")
    print(f"manual_parent={manual_parent.name}")
    print(f"manual_span={manual_span.name}")
    print(f"tracking_uri={mlflow.get_tracking_uri()}")


def run(check: bool) -> None:
    experiment_id = _configure_mlflow()

    DBOS(
        config={
            "name": "dbos-mlflow-propagation",
            "system_database_url": f"sqlite+pysqlite:///{_DB_FILE}?check_same_thread=false",
            "run_admin_server": False,
        }
    )

    runtime = DBOSRuntime()
    runtime.launch_sync()

    run_id = f"mlflow-{uuid.uuid4().hex[:8]}"
    try:
        with mlflow.start_span("request-root") as root:
            result = asyncio.run(_run_workflow(runtime, run_id))
            root.set_outputs({"result": result.final_result})
            trace_id = root.trace_id
            root_span_id = root.span_id
        print(f"result={result.final_result}")
    finally:
        runtime.destroy_sync()

    if check:
        _check_trace(experiment_id, trace_id, root_span_id)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="DBOS + MLflow trace propagation example"
    )
    parser.add_argument(
        "--check", action="store_true", help="Verify the emitted MLflow trace"
    )
    parser.add_argument(
        "--clean", action="store_true", help="Remove local SQLite and MLflow data"
    )
    args = parser.parse_args()

    if args.clean:
        for file in (_DB_FILE, _MLFLOW_DB_FILE):
            if file.exists():
                file.unlink()
        if _MLRUNS_DIR.exists():
            shutil.rmtree(_MLRUNS_DIR)
        return

    run(check=args.check)


if __name__ == "__main__":
    main()
