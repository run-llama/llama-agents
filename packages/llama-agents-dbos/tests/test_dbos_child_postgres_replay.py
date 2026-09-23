# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""PostgreSQL replay of overlapping child invocations."""

from __future__ import annotations

import pytest
from tests.conftest import assert_no_determinism_errors, run_scenario


@pytest.mark.docker
def test_overlapping_children_resume_after_crash(postgres_dsn: str) -> None:
    workflow = (
        "tests.fixtures.sample_workflows.overlapping_children:OverlappingChildren"
    )
    run_id = "test-overlapping-children-replay-001"
    first = run_scenario(
        workflow=workflow,
        db_url=postgres_dsn,
        run_id=run_id,
        config={"interrupt_on": "FirstChildFinished"},
    )
    assert "INTERRUPTING" in first.stdout, first.stdout + first.stderr

    resumed = run_scenario(workflow=workflow, db_url=postgres_dsn, run_id=run_id)
    assert_no_determinism_errors(resumed)
    assert "RESULT:['fast', 'slow']" in resumed.stdout, first.stdout + resumed.stdout
    assert "SUCCESS" in resumed.stdout
