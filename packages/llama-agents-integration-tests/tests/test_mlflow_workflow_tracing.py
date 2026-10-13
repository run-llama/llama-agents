# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
import uuid
from pathlib import Path
from typing import cast

import mlflow
from dbos import DBOS
from llama_agents.dbos import DBOSRuntime
from mlflow.entities import Span
from mlflow.entities.trace import Trace
from mlflow.llama_index import autolog as mlflow_llama_index_autolog
from pydantic import Field
from workflows import Context, Workflow, step
from workflows.events import (
    HumanResponseEvent,
    InputRequiredEvent,
    StartEvent,
    StopEvent,
)
from workflows.handler import WorkflowHandler


class WorkStart(StartEvent):
    value: str = Field(description="Value to process")


class NeedInput(InputRequiredEvent):
    prompt: str = "continue?"


class UserInput(HumanResponseEvent):
    response: str = "dbos"


class WorkResult(StopEvent):
    final_result: str = Field(description="Processed value")


class TracePropagationWorkflow(Workflow):
    @step
    async def ask(self, ctx: Context, ev: WorkStart) -> NeedInput:
        await ctx.store.set("value", ev.value)
        return NeedInput()

    @step
    async def process(self, ctx: Context, ev: UserInput) -> WorkResult:
        value = await ctx.store.get("value", default=ev.response)
        result = str(value).upper()

        with mlflow.start_span("manual-dbospan") as span:
            span.set_inputs({"value": value})
            span.set_outputs({"result": result})

        return WorkResult(final_result=result)


def _configure_mlflow(tracking_uri: str, experiment_name: str) -> str:
    mlflow.set_tracking_uri(tracking_uri)
    experiment = mlflow.set_experiment(experiment_name)
    mlflow_llama_index_autolog()
    return experiment.experiment_id


def _configure_dbos(db_file: Path) -> DBOSRuntime:
    DBOS(
        config={
            "name": "mlflow-dbos-context",
            "system_database_url": f"sqlite+pysqlite:///{db_file}?check_same_thread=false",
            "run_admin_server": False,
        }
    )
    return DBOSRuntime(polling_interval_sec=0.05)


async def _wait_for_need_input(handler: WorkflowHandler) -> None:
    async for event in handler.stream_events():
        print(f"EVENT:{type(event).__name__}", flush=True)
        if isinstance(event, NeedInput):
            return
    raise RuntimeError("workflow completed before NeedInput")


async def _phase_start(
    db_file: Path,
    tracking_uri: str,
    experiment_name: str,
    run_id: str,
) -> None:
    experiment_id = _configure_mlflow(tracking_uri, experiment_name)
    runtime = _configure_dbos(db_file)
    workflow = TracePropagationWorkflow(runtime=runtime, timeout=None)
    await runtime.launch()

    with mlflow.start_span("request-root") as root:
        handler = workflow.run(start_event=WorkStart(value="dbos"), run_id=run_id)
        if not isinstance(handler, WorkflowHandler):
            raise TypeError(f"Expected WorkflowHandler, got {type(handler)!r}")
        await asyncio.wait_for(_wait_for_need_input(handler), timeout=20)
        root.set_outputs({"run_id": run_id, "waiting": True})

    _search_traces(experiment_id)
    print("PHASE:start:interrupted-after-input-required", flush=True)
    os._exit(0)


async def _phase_resume(
    db_file: Path,
    tracking_uri: str,
    experiment_name: str,
    run_id: str,
) -> None:
    _configure_mlflow(tracking_uri, experiment_name)
    runtime = _configure_dbos(db_file)
    workflow = TracePropagationWorkflow(runtime=runtime, timeout=None)
    await runtime.launch()

    try:
        existing = await DBOS.get_workflow_status_async(run_id)
        if existing is None:
            raise RuntimeError(f"DBOS workflow {run_id!r} was not found")

        handler = WorkflowHandler(workflow, runtime.get_external_adapter(run_id))
        sent = False
        async for event in handler.stream_events():
            print(f"EVENT:{type(event).__name__}", flush=True)
            if isinstance(event, NeedInput) and not sent:
                handler.ctx.send_event(UserInput(response="dbos"))
                sent = True

        result = await handler
        if not isinstance(result, WorkResult):
            raise TypeError(f"Expected WorkResult, got {type(result)!r}")
        print(f"RESULT:{result.final_result}", flush=True)
    finally:
        await runtime.destroy()


def _search_traces(experiment_id: str) -> list[Trace]:
    try:
        return cast(
            list[Trace],
            mlflow.search_traces(
                locations=[experiment_id],
                return_type="list",
                include_spans=True,
                flush=True,
            ),
        )
    except TypeError:
        return cast(
            list[Trace],
            mlflow.search_traces(
                experiment_ids=[experiment_id],
                return_type="list",
                include_spans=True,
            ),
        )


def _span_named(trace: Trace, name: str) -> Span:
    matches = [span for span in trace.data.spans if span.name == name]
    assert len(matches) == 1, (
        f"expected one span named {name!r}, got {[span.name for span in trace.data.spans]}"
    )
    return matches[0]


def _span_by_id(trace: Trace, span_id: str | None) -> Span | None:
    if span_id is None:
        return None
    return next(
        (span for span in trace.data.spans if span.span_id == span_id),
        None,
    )


def _span_named_across_traces(traces: list[Trace], name: str) -> tuple[Trace, Span]:
    matches = [
        (trace, span)
        for trace in traces
        for span in trace.data.spans
        if span.name == name
    ]
    assert len(matches) == 1, (
        f"expected one span named {name!r}, got "
        f"{[(trace.info.trace_id, span.name) for trace, span in matches]}"
    )
    return matches[0]


def _format_traces(traces: list[Trace]) -> str:
    lines: list[str] = []
    for trace in traces:
        lines.append(f"trace_id={trace.info.trace_id}")
        for span in trace.data.spans:
            parent_id = span.parent_id or "<none>"
            lines.append(f"  {span.name} span_id={span.span_id} parent_id={parent_id}")
    return "\n".join(lines)


def _run_child(
    phase: str,
    db_file: Path,
    tracking_uri: str,
    experiment_name: str,
    run_id: str,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--phase",
            phase,
            "--db-file",
            str(db_file),
            "--tracking-uri",
            tracking_uri,
            "--experiment-name",
            experiment_name,
            "--run-id",
            run_id,
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def test_mlflow_context_propagates_through_dbos_recovery(tmp_path: Path) -> None:
    db_file = tmp_path / "dbos.sqlite3"
    tracking_uri = f"sqlite:///{tmp_path / 'mlflow.sqlite3'}"
    experiment_name = f"workflow-tracing-{uuid.uuid4().hex}"
    run_id = f"mlflow-dbos-{uuid.uuid4().hex[:8]}"

    start = _run_child("start", db_file, tracking_uri, experiment_name, run_id)
    assert start.returncode == 0, start.stdout + start.stderr
    assert "PHASE:start:interrupted-after-input-required" in start.stdout

    resume = _run_child("resume", db_file, tracking_uri, experiment_name, run_id)
    assert resume.returncode == 0, resume.stdout + resume.stderr
    assert "RESULT:DBOS" in resume.stdout

    mlflow.set_tracking_uri(tracking_uri)
    experiment = mlflow.get_experiment_by_name(experiment_name)
    assert experiment is not None

    traces = _search_traces(experiment.experiment_id)
    root_trace, root_span = _span_named_across_traces(traces, "request-root")
    manual_trace, manual_span = _span_named_across_traces(traces, "manual-dbospan")

    assert manual_trace.info.trace_id == root_trace.info.trace_id, _format_traces(
        traces
    )

    _span_named(root_trace, "TracePropagationWorkflow.ask")
    process_span = _span_named(root_trace, "TracePropagationWorkflow.process")
    workflow_span = _span_by_id(root_trace, process_span.parent_id)

    assert workflow_span is not None
    assert workflow_span.name == "TracePropagationWorkflow.run"
    assert workflow_span.parent_id == root_span.span_id
    assert manual_span.parent_id == process_span.span_id


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=("start", "resume"), required=True)
    parser.add_argument("--db-file", type=Path, required=True)
    parser.add_argument("--tracking-uri", required=True)
    parser.add_argument("--experiment-name", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    if args.phase == "start":
        asyncio.run(
            _phase_start(
                args.db_file,
                args.tracking_uri,
                args.experiment_name,
                args.run_id,
            )
        )
    else:
        asyncio.run(
            _phase_resume(
                args.db_file,
                args.tracking_uri,
                args.experiment_name,
                args.run_id,
            )
        )


if __name__ == "__main__":
    main()
