"""Tests for workflow Context store operations.

These tests verify that ctx.store.get/set work correctly when used
by llama-index agents, including state persistence across steps and
access from within tool functions.
"""

import threading

from conftest import WorkflowFactory
from llama_agents_integration_tests.helpers import (
    make_text_response,
    make_tool_call_response,
    response_generator_from_list,
)
from llama_index.core.agent.workflow import (
    AgentInput,
    AgentStream,
    AgentWorkflow,
    FunctionAgent,
    ReActAgent,
    ToolCall,
)
from llama_index.core.base.llms.types import ChatMessage, MessageRole
from llama_index.core.llms.mock import MockFunctionCallingLLM
from llama_index.core.memory import Memory
from workflows import Context


def _mock_agent(
    name: str,
    description: str,
    responses: list[ChatMessage],
    can_handoff_to: list[str] | None = None,
) -> FunctionAgent:
    """Build a FunctionAgent whose mock LLM cycles through fixed responses."""
    llm = MockFunctionCallingLLM(
        response_generator=response_generator_from_list(responses)
    )
    return FunctionAgent(
        name=name,
        description=description,
        llm=llm,
        can_handoff_to=can_handoff_to or [],
    )


async def test_unhashable_react_agent_runs_with_default_context() -> None:
    llm = MockFunctionCallingLLM(
        response_generator=response_generator_from_list([make_text_response("Done")])
    )
    agent = ReActAgent(
        name="test_agent",
        description="Test agent",
        llm=llm,
    )

    assert type(agent).__hash__ is None
    result = await agent.run(user_msg="hello")

    assert result.response.content == "Done"


async def test_multi_agent_handoff_streams_with_memory() -> None:
    """A streamed handoff preserves the supplied memory."""
    router = _mock_agent(
        "router",
        "Routes the chat to a specialist.",
        responses=[
            make_tool_call_response(
                "handoff",
                {"to_agent": "specialist", "reason": "needs specialist"},
            ),
        ],
        can_handoff_to=["specialist"],
    )
    specialist = _mock_agent(
        "specialist",
        "Answers the question.",
        responses=[make_text_response("specialist answer")],
    )
    workflow = AgentWorkflow(agents=[router, specialist], root_agent="router")
    memory = Memory.from_defaults(
        chat_history=[ChatMessage(role=MessageRole.USER, content="earlier turn")]
    )

    handler = workflow.run(user_msg="help me", memory=memory)
    active_agents: list[str] = []
    handoff_tool_calls: list[str] = []
    specialist_stream = ""
    async for event in handler.stream_events():
        if isinstance(event, AgentInput):
            active_agents.append(event.current_agent_name)
        elif isinstance(event, ToolCall):
            handoff_tool_calls.append(event.tool_name)
        elif (
            isinstance(event, AgentStream) and event.current_agent_name == "specialist"
        ):
            specialist_stream += event.delta
    result = await handler

    # Router runs, hands off, then the specialist takes over and answers.
    assert active_agents == ["router", "specialist"]
    assert "handoff" in handoff_tool_calls
    assert specialist_stream == "specialist answer"
    assert result.response.content == "specialist answer"

    stored_memory = await handler.ctx.store.get("memory")
    assert isinstance(stored_memory, Memory)
    assert stored_memory.tokenizer_fn is memory.tokenizer_fn
    assert stored_memory.sql_store is memory.sql_store
    messages = stored_memory.get_all()
    assert messages == memory.get_all()
    assert messages[0].content == "earlier turn"
    assert any(message.content == "help me" for message in messages)
    assert messages[-1].content == "specialist answer"


async def test_store_set_accepts_non_serializable_object(
    create_workflow: WorkflowFactory,
) -> None:
    """The store keeps an uncopyable live object by reference."""
    lock = threading.Lock()
    captured = None

    async def stash_client(ctx: Context) -> str:
        nonlocal captured
        await ctx.store.set("client", lock)
        captured = await ctx.store.get("client")
        return "stored"

    workflow = create_workflow(
        tools=[stash_client],
        responses=[
            make_tool_call_response("stash_client"),
            make_text_response("Done"),
        ],
    )

    handler = workflow.run(user_msg="stash it")
    async for _ in handler.stream_events():
        pass
    await handler

    assert captured is lock


async def test_initial_state_accessible_in_tool(
    create_workflow: WorkflowFactory,
) -> None:
    """Test that initial_state is accessible via ctx.store in tools."""
    received_value = None

    async def check_state(ctx: Context) -> str:
        nonlocal received_value
        state = await ctx.store.get("state")
        received_value = state.get("initial_key")
        return f"Got: {received_value}"

    workflow = create_workflow(
        tools=[check_state],
        responses=[
            make_tool_call_response("check_state"),
            make_text_response("Done"),
        ],
        initial_state={"initial_key": "initial_value"},
    )

    handler = workflow.run(user_msg="Check the state")
    async for _ in handler.stream_events():
        pass
    await handler

    assert received_value == "initial_value"


async def test_state_modification_persists(create_workflow: WorkflowFactory) -> None:
    """Test that state modifications in tools persist across calls."""
    call_count = 0
    final_counter_value = None

    async def increment_counter(ctx: Context) -> str:
        nonlocal call_count, final_counter_value
        call_count += 1
        state = await ctx.store.get("state")
        state["counter"] = state.get("counter", 0) + 1
        await ctx.store.set("state", state)
        final_counter_value = state["counter"]
        return f"Counter: {state['counter']}"

    workflow = create_workflow(
        tools=[increment_counter],
        responses=[
            make_tool_call_response("increment_counter"),
            make_tool_call_response("increment_counter"),
            make_text_response("Done"),
        ],
        initial_state={"counter": 0},
    )

    handler = workflow.run(user_msg="Increment twice")
    async for _ in handler.stream_events():
        pass
    await handler

    # Verify tool was called twice
    assert call_count == 2

    # Verify final state via the last captured value
    assert final_counter_value == 2


async def test_state_survives_handler_access(
    create_workflow: WorkflowFactory,
) -> None:
    """Test that state can be read from handler.ctx after workflow completes."""
    captured_result = None

    async def set_result(ctx: Context) -> str:
        nonlocal captured_result
        state = await ctx.store.get("state")
        state["result"] = "computation_complete"
        await ctx.store.set("state", state)
        captured_result = state["result"]
        return "Done"

    workflow = create_workflow(
        tools=[set_result],
        responses=[
            make_tool_call_response("set_result"),
            make_text_response("Finished"),
        ],
        initial_state={},
    )

    handler = workflow.run(user_msg="Compute something")
    async for _ in handler.stream_events():
        pass
    await handler

    # Verify state was set correctly via captured value
    assert captured_result == "computation_complete"


async def test_complex_state_types(create_workflow: WorkflowFactory) -> None:
    """Test that complex state types (nested dicts, lists) work correctly."""
    captured_state: dict | None = None

    async def modify_complex_state(ctx: Context) -> str:
        nonlocal captured_state
        state = await ctx.store.get("state")
        state["items"].append("new_item")
        state["nested"]["count"] += 1
        await ctx.store.set("state", state)
        # Capture a copy of the state for verification
        captured_state = {
            "items": list(state["items"]),
            "nested": dict(state["nested"]),
        }
        return "Modified"

    workflow = create_workflow(
        tools=[modify_complex_state],
        responses=[
            make_tool_call_response("modify_complex_state"),
            make_text_response("Done"),
        ],
        initial_state={
            "items": ["initial"],
            "nested": {"count": 0, "name": "test"},
        },
    )

    handler = workflow.run(user_msg="Modify state")
    async for _ in handler.stream_events():
        pass
    await handler

    assert captured_state is not None
    assert captured_state["items"] == ["initial", "new_item"]
    assert captured_state["nested"]["count"] == 1
    assert captured_state["nested"]["name"] == "test"  # Unchanged


async def test_multiple_tools_share_state(create_workflow: WorkflowFactory) -> None:
    """Test that multiple different tools can share state."""
    final_state: dict | None = None

    async def tool_a(ctx: Context) -> str:
        state = await ctx.store.get("state")
        state["from_a"] = True
        await ctx.store.set("state", state)
        return "A done"

    async def tool_b(ctx: Context) -> str:
        nonlocal final_state
        state = await ctx.store.get("state")
        state["from_b"] = True
        # Verify tool_a's change is visible
        assert state.get("from_a") is True
        await ctx.store.set("state", state)
        final_state = dict(state)
        return "B done"

    workflow = create_workflow(
        tools=[tool_a, tool_b],
        responses=[
            make_tool_call_response("tool_a"),
            make_tool_call_response("tool_b"),
            make_text_response("Done"),
        ],
        initial_state={},
    )

    handler = workflow.run(user_msg="Run both tools")
    async for _ in handler.stream_events():
        pass
    await handler

    assert final_state is not None
    assert final_state["from_a"] is True
    assert final_state["from_b"] is True
