# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from collections.abc import Iterable

from workflows.decorators import CatchErrorHandler, StepConfig
from workflows.errors import WorkflowValidationError


def validate_catch_error_handlers(
    handlers: Iterable[CatchErrorHandler],
    step_names: set[str],
) -> list[str]:
    """Validate structural invariants of ``@catch_error`` handlers.

    Returns a list of error messages; empty when the handler set is valid.
    Callers are responsible for raising.
    """
    errors: list[str] = []

    handlers = list(handlers)
    wildcard_handlers = [h for h in handlers if h.for_steps is None]
    if len(wildcard_handlers) > 1:
        names = ", ".join(sorted(h.step_name for h in wildcard_handlers))
        errors.append(
            f"Only one wildcard @catch_error handler is allowed per workflow, "
            f"found {len(wildcard_handlers)}: {names}"
        )

    handler_step_names = {h.step_name for h in handlers}
    claim_owner: dict[str, str] = {}
    for handler in handlers:
        if handler.for_steps is None:
            continue
        for target in handler.for_steps:
            if target not in step_names:
                errors.append(
                    f"@catch_error handler '{handler.step_name}' lists "
                    f"unknown step '{target}' in for_steps."
                )
                continue
            if target == handler.step_name or target in handler_step_names:
                errors.append(
                    f"@catch_error handler '{handler.step_name}' cannot "
                    f"cover another handler step '{target}'."
                )
                continue
            if target in claim_owner:
                errors.append(
                    f"Step '{target}' is claimed by two @catch_error "
                    f"handlers: '{claim_owner[target]}' and '{handler.step_name}'."
                )
                continue
            claim_owner[target] = handler.step_name

    return errors


def _collect_catch_error_handlers(
    steps: dict[str, StepConfig],
) -> tuple[dict[str, CatchErrorHandler], dict[str, str]]:
    """Discover ``@catch_error`` handlers and build the step->handler routing table.

    Validates the handler set (via :func:`validate_catch_error_handlers`) and
    each handler's ``max_recoveries``; raises ``WorkflowValidationError`` on
    any problem.

    Returns ``(catch_error_handlers, handler_for_step)`` where
    ``catch_error_handlers`` maps handler step name to its descriptor and
    ``handler_for_step`` maps each covered step name to the handler that owns
    it (scoped claims first, then the wildcard fills).
    """
    all_step_names = set(steps.keys())
    handlers: list[CatchErrorHandler] = []
    for name, cfg in steps.items():
        if cfg.role != "catch_error":
            continue
        max_recoveries = cfg.catch_error_max_recoveries
        if not isinstance(max_recoveries, int) or max_recoveries < 1:
            raise WorkflowValidationError(
                f"@catch_error handler '{name}' has max_recoveries="
                f"{max_recoveries!r}; must be an integer >= 1."
            )
        handlers.append(
            CatchErrorHandler(
                step_name=name,
                for_steps=(
                    list(cfg.catch_error_for_steps)
                    if cfg.catch_error_for_steps is not None
                    else None
                ),
                max_recoveries=max_recoveries,
            )
        )

    handler_errors = validate_catch_error_handlers(handlers, all_step_names)
    if handler_errors:
        raise WorkflowValidationError("\n".join(handler_errors))

    handler_step_names = {h.step_name for h in handlers}
    handler_for_step: dict[str, str] = {}
    for handler in handlers:
        if handler.for_steps is None:
            continue
        for target in handler.for_steps:
            handler_for_step[target] = handler.step_name

    wildcards = [h for h in handlers if h.for_steps is None]
    wildcard = wildcards[0] if wildcards else None
    if wildcard is not None:
        for step_name in all_step_names:
            if step_name in handler_step_names:
                continue
            if step_name in handler_for_step:
                continue
            handler_for_step[step_name] = wildcard.step_name

    return {h.step_name: h for h in handlers}, handler_for_step
