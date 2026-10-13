# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

import random
from datetime import datetime
from uuid import UUID

from workflows import Context, Workflow, step
from workflows.events import Event, StartEvent, StopEvent


class InputEvent(StartEvent):
    greeting: str
    name: str


class GreetEvent(Event):
    greeting: str
    exclamation_marks: int


class OutputEvent(StopEvent):
    greeting: str


class GreetingWorkflow(Workflow):
    @step
    async def first_step(self, ev: InputEvent, ctx: Context) -> GreetEvent:
        ctx.write_event_to_stream(ev)
        return GreetEvent(
            greeting=f"{ev.greeting} {ev.name}", exclamation_marks=random.randint(1, 10)
        )

    @step
    async def second_step(self, ev: GreetEvent, ctx: Context) -> OutputEvent:
        ctx.write_event_to_stream(ev)
        return OutputEvent(greeting=f"{ev.greeting}{'!' * ev.exclamation_marks}")


class CrashingWorkflow(Workflow):
    @step
    async def crashing_step(self, ev: StartEvent) -> StopEvent:
        raise ValueError("Workflow crashed intentionally")


class TypedInputEvent(StartEvent):
    timestamp: datetime
    request_id: UUID


class TypedInputWorkflow(Workflow):
    @step
    async def echo(self, ev: TypedInputEvent) -> StopEvent:
        return StopEvent(result=ev.model_dump(mode="json"))
