# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from collections import Counter
from dataclasses import dataclass
from typing import Any, Optional

from workflows import Context, Workflow
from workflows.events import Event, EventType, StartEvent


@dataclass
class WorkflowTestResult:
    """
    Container for workflow test results

    Attributes:
        collected (list[Event]): List of collected events
        event_type (dict[EventType, int]): Dictionary that maps each event type with its number of occurencies within the collected events
        result (Any): Final output of the workflow run
    """

    collected: list[Event]
    event_types: dict[EventType, int]
    result: Any
    ctx: Context


class WorkflowTestRunner:
    """
    Utility class that can be used to test workflows end-to-end.

    Attributes:
        _workflow (Workflow): The workflow to be tested
    """

    def __init__(
        self,
        workflow: "Workflow",
    ):
        self._workflow = workflow

    async def run(
        self,
        start_event: StartEvent | None = None,
        ctx: Optional["Context"] = None,
        expose_internal: bool = True,
        exclude_events: list[EventType] | None = None,
    ) -> WorkflowTestResult:
        """
        Run a workflow end-to-end and collect the events that are streamed during its execution.

        Args:
            start_event (StartEvent | None): The input event for the workflow.
                If omitted, the workflow creates a fresh event of its declared start type.
            expose_internal (bool): Whether or not to expose internal events. Defaults to True if not set.
            exclude_events. (list[EventType]): A list of event types to exclude from the collected events. Defaults to None if not set.

        Returns:
            WorkflowTestResult

        Example:
            ```
            wf = GreetingWorkflow()
            runner = WorkflowTestRunner(wf)
            test_result = runner.run(start_even=StartEvent(message="hello"), expose_internal = True, exclude_events = [StepStateChanged])
            assert test_result.collected == 22
            assert test_result.event_types.get(StepStateChanged, 0) == 8
            assert str(test_result.result) == "hello Adam!"
            ```
        """
        handler = self._workflow.run(start_event=start_event, ctx=ctx)
        collected_events: list[Event] = []
        async for event in handler.stream_events(expose_internal=expose_internal):
            if exclude_events and type(event) in exclude_events:
                continue
            collected_events.append(event)
        result = await handler
        event_freqs: dict[EventType, int] = dict(
            Counter([type(ev) for ev in collected_events])
        )
        assert handler.ctx is not None
        return WorkflowTestResult(
            collected=collected_events,
            result=result,
            event_types=event_freqs,
            ctx=handler.ctx,
        )
