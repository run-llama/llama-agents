import asyncio
import pytest
from typing import Any, Dict, List
from agentfile.services import ToolService
from agentfile.message_queues.simple import SimpleMessageQueue
from agentfile.message_consumers.base import BaseMessageQueueConsumer
from agentfile.messages.base import QueueMessage
from agentfile.tools import MetaServiceTool
from llama_index.core.bridge.pydantic import PrivateAttr
from llama_index.core.tools import FunctionTool, BaseTool


class MockMessageConsumer(BaseMessageQueueConsumer):
    processed_messages: List[QueueMessage] = []
    _lock: asyncio.Lock = PrivateAttr(default_factory=asyncio.Lock)

    async def _process_message(self, message: QueueMessage, **kwargs: Any) -> None:
        async with self._lock:
            self.processed_messages.append(message)


@pytest.fixture()
def tools() -> List[BaseTool]:
    def multiply(a: int, b: int) -> int:
        """Multiple two integers and returns the result integer"""
        return a * b

    return [FunctionTool.from_defaults(fn=multiply)]


@pytest.fixture()
def message_queue() -> SimpleMessageQueue:
    return SimpleMessageQueue()


@pytest.fixture()
def tool_service(
    message_queue: SimpleMessageQueue, tools: List[BaseTool]
) -> ToolService:
    return ToolService(
        message_queue=message_queue,
        tools=tools,
        running=True,
        service_name="test_tool_service",
        description="Test Tool Server",
        step_interval=0.5,
    )


@pytest.mark.asyncio()
async def test_init(
    message_queue: SimpleMessageQueue, tools: List[BaseTool], tool_service: ToolService
) -> None:
    # arrange
    result = await tool_service.get_tool_by_name("multiply")

    # act
    meta_service_tool = MetaServiceTool(
        tool_metadata=result["tool_metadata"],
        message_queue=message_queue,
        tool_service_name=tool_service.service_name,
    )

    # assert
    assert meta_service_tool.metadata.name == "multiply"


@pytest.mark.asyncio()
async def test_create_from_tool_service_direct(
    message_queue: SimpleMessageQueue, tool_service: ToolService
) -> None:
    # arrange

    # act
    meta_service_tool: MetaServiceTool = await MetaServiceTool.from_tool_service(
        tool_service=tool_service, message_queue=message_queue, name="multiply"
    )

    # assert
    assert meta_service_tool.metadata.name == "multiply"


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    ("from_tool_service_kwargs"),
    [
        {"message_queue": SimpleMessageQueue(), "name": "multiply"},
        {
            "message_queue": SimpleMessageQueue(),
            "name": "multiply",
            "tool_service_name": "fake-name",
        },
        {
            "message_queue": SimpleMessageQueue(),
            "name": "multiply",
            "tool_service_api_key": "fake-key",
        },
        {
            "message_queue": SimpleMessageQueue(),
            "name": "multiply",
            "tool_service_url": "fake-url",
        },
        {
            "message_queue": SimpleMessageQueue(),
            "name": "multiply",
            "tool_service_name": "fake-name",
            "tool_service_api_key": "fake-key",
        },
        {
            "message_queue": SimpleMessageQueue(),
            "name": "multiply",
            "tool_service_name": "fake-name",
            "tool_service_url": "fake-url",
        },
        {
            "message_queue": SimpleMessageQueue(),
            "name": "multiply",
            "tool_service_api_key": "fake-key",
            "tool_service_url": "fake-url",
        },
    ],
)
async def test_create_from_tool_service_raise_error(
    from_tool_service_kwargs: Dict[str, Any],
) -> None:
    # arrange
    # act/assert
    with pytest.raises(ValueError):
        await MetaServiceTool.from_tool_service(**from_tool_service_kwargs)


@pytest.mark.asyncio()
async def test_tool_call_output(
    message_queue: SimpleMessageQueue, tool_service: ToolService
) -> None:
    # arrange
    meta_service_tool: MetaServiceTool = await MetaServiceTool.from_tool_service(
        tool_service=tool_service, message_queue=message_queue, name="multiply"
    )
    await message_queue.register_consumer(meta_service_tool.as_consumer())
    await message_queue.register_consumer(tool_service.as_consumer())
    mq_task = asyncio.create_task(message_queue.start())
    ts_task = asyncio.create_task(tool_service.processing_loop())

    # act
    tool_output = await meta_service_tool.acall(a=1, b=9)

    # clean-up/shutdown
    await asyncio.sleep(0.5)
    mq_task.cancel()
    ts_task.cancel()

    # assert
    assert tool_output.content == "9"
    assert tool_output.tool_name == "multiply"
    assert tool_output.raw_input == {"args": (), "kwargs": {"a": 1, "b": 9}}
    assert len(meta_service_tool.tool_call_results) == 0


@pytest.mark.asyncio()
async def test_tool_call_timeout(
    message_queue: SimpleMessageQueue, tool_service: ToolService
) -> None:
    # arrange
    meta_service_tool: MetaServiceTool = await MetaServiceTool.from_tool_service(
        tool_service=tool_service,
        message_queue=message_queue,
        name="multiply",
        timeout=1e-9,
    )
    await message_queue.register_consumer(meta_service_tool.as_consumer())
    await message_queue.register_consumer(tool_service.as_consumer())
    mq_task = asyncio.create_task(message_queue.start())
    ts_task = asyncio.create_task(tool_service.processing_loop())

    # act/assert
    with pytest.raises(asyncio.exceptions.TimeoutError):
        await meta_service_tool.acall(a=1, b=9)

    mq_task.cancel()
    ts_task.cancel()
