"""Shared fixtures for llama-index workflow integration tests."""

from __future__ import annotations

from typing import Any, Callable, Generator, Protocol

import pytest
from llama_agents_integration_tests.helpers import (
    make_text_response,
    response_generator_from_list,
)
from llama_agents_integration_tests.postgres import (
    postgres_container as _postgres_container,
)
from llama_index.core.agent.workflow import (
    AgentWorkflow,
    FunctionAgent,
    ReActAgent,
)
from llama_index.core.base.llms.types import ChatMessage
from llama_index.core.llms.mock import MockFunctionCallingLLM
from llama_index.core.tools import BaseTool
from sqlalchemy.engine import Engine
from testcontainers.postgres import PostgresContainer


class WorkflowFactory(Protocol):
    """Protocol for workflow factory fixtures."""

    def __call__(
        self,
        name: str = ...,
        tools: list[BaseTool | Callable[..., Any]] | None = ...,
        responses: list[ChatMessage] | None = ...,
        initial_state: dict[str, Any] | None = ...,
        **kwargs: Any,
    ) -> AgentWorkflow: ...


class SimpleWorkflowFactory(Protocol):
    """Protocol for simple workflow factory fixtures."""

    def __call__(
        self,
        name: str = ...,
        responses: list[ChatMessage] | None = ...,
        **kwargs: Any,
    ) -> AgentWorkflow: ...


@pytest.fixture
def create_workflow() -> WorkflowFactory:
    """Factory fixture to create AgentWorkflow with FunctionAgent.

    FunctionAgent is used because it supports function calling via
    tool_calls in additional_kwargs, which our mock responses use.
    """

    def _create(
        name: str = "test_agent",
        tools: list[BaseTool | Callable[..., Any]] | None = None,
        responses: list[ChatMessage] | None = None,
        initial_state: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> AgentWorkflow:
        if responses is None:
            responses = [make_text_response("Done")]

        llm = MockFunctionCallingLLM(
            response_generator=response_generator_from_list(responses)
        )

        agent = FunctionAgent(
            name=name,
            description=f"Test {name}",
            tools=tools or [],
            llm=llm,
        )

        return AgentWorkflow(
            agents=[agent],
            root_agent=name,
            initial_state=initial_state,
            **kwargs,
        )

    return _create  # type: ignore[return-value]


@pytest.fixture(params=["function", "react"])
def agent_type(request: pytest.FixtureRequest) -> str:
    """Parameterized fixture for agent type.

    Use this only for tests that don't involve tool invocations,
    since ReActAgent uses text parsing instead of function calling.
    """
    return request.param  # type: ignore[return-value]


@pytest.fixture
def create_simple_workflow(agent_type: str) -> SimpleWorkflowFactory:
    """Factory for simple workflows (no tools) parameterized by agent type.

    Use this for tests that want to verify behavior across both
    FunctionAgent and ReActAgent without tool invocations.
    """

    def _create(
        name: str = "test_agent",
        responses: list[ChatMessage] | None = None,
        **kwargs: Any,
    ) -> AgentWorkflow:
        if responses is None:
            responses = [make_text_response("Done")]

        llm = MockFunctionCallingLLM(
            response_generator=response_generator_from_list(responses)
        )

        if agent_type == "function":
            agent = FunctionAgent(
                name=name,
                description=f"Test {name}",
                llm=llm,
            )
        else:
            agent = ReActAgent(
                name=name,
                description=f"Test {name}",
                llm=llm,
            )

        return AgentWorkflow(
            agents=[agent],
            root_agent=name,
            **kwargs,
        )

    return _create  # type: ignore[return-value]


# -- Docker/PostgreSQL Fixtures --


@pytest.fixture(scope="module")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Module-scoped PostgreSQL container for integration tests.

    Requires Docker to be running. Used by tests marked with @pytest.mark.docker.
    """
    with _postgres_container() as pg:
        yield pg


@pytest.fixture(scope="module")
def postgres_engine(
    postgres_container: PostgresContainer,
) -> Generator[Engine, None, None]:
    """Module-scoped PostgreSQL engine for integration tests."""
    from sqlalchemy import create_engine

    # Get connection URL and convert to use psycopg (psycopg3) driver
    connection_url = postgres_container.get_connection_url()
    # Replace postgresql:// or postgresql+psycopg2:// with postgresql+psycopg://
    if "postgresql+psycopg2://" in connection_url:
        connection_url = connection_url.replace(
            "postgresql+psycopg2://", "postgresql+psycopg://"
        )
    elif connection_url.startswith("postgresql://"):
        connection_url = connection_url.replace(
            "postgresql://", "postgresql+psycopg://", 1
        )
    engine = create_engine(connection_url)
    yield engine
    engine.dispose()
