import asyncio
import pytest
from typing import Any, List
from unittest.mock import MagicMock, patch
from agentfile.services import HumanService
from agentfile.services.human import HELP_REQUEST_TEMPLATE_STR
from agentfile.message_queues.simple import SimpleMessageQueue
from agentfile.message_consumers.base import BaseMessageQueueConsumer
from agentfile.messages.base import QueueMessage
from agentfile.types import HumanRequest
from llama_index.core.bridge.pydantic import PrivateAttr


HUMAN_REQ_SOURCE = "mock-source"


class MockMessageConsumer(BaseMessageQueueConsumer):
    processed_messages: List[QueueMessage] = []
    _lock: asyncio.Lock = PrivateAttr(default_factory=asyncio.Lock)

    async def _process_message(self, message: QueueMessage, **kwargs: Any) -> None:
        async with self._lock:
            self.processed_messages.append(message)


@pytest.fixture()
def human_output_consumer() -> MockMessageConsumer:
    return MockMessageConsumer(message_type=HUMAN_REQ_SOURCE)


@pytest.mark.asyncio()
async def test_init() -> None:
    # arrange
    # act
    human_service = HumanService(
        message_queue=SimpleMessageQueue(),
        running=False,
        description="Test Human Service",
        service_name="Test Human Service",
        step_interval=0.5,
    )

    # assert
    assert not human_service.running
    assert human_service.description == "Test Human Service"
    assert human_service.service_name == "Test Human Service"
    assert human_service.step_interval == 0.5


@pytest.mark.asyncio()
async def test_create_human_req() -> None:
    # arrange
    human_service = HumanService(
        message_queue=SimpleMessageQueue(),
        running=False,
        description="Test Human Service",
        service_name="Test Human Service",
        step_interval=0.5,
    )
    req = HumanRequest(id_="1", input="Mock human req.", source_id="another_human")

    # act
    result = await human_service.create_human_request(req)

    # assert
    assert result == {"human_request_id": req.id_}
    assert human_service._outstanding_human_requests[req.id_] == req


@pytest.mark.asyncio()
@patch("builtins.input")
async def test_process_human_req(
    mock_input: MagicMock, human_output_consumer: MockMessageConsumer
) -> None:
    # arrange
    mq = SimpleMessageQueue()
    human_service = HumanService(
        message_queue=mq,
    )
    await mq.register_consumer(human_output_consumer)

    mq_task = asyncio.create_task(mq.start())
    server_task = asyncio.create_task(human_service.processing_loop())
    mock_input.return_value = "Test human input."

    # act
    req = HumanRequest(id_="1", input="Mock human req.", source_id=HUMAN_REQ_SOURCE)
    result = await human_service.create_human_request(req)

    # give time to process and shutdown afterwards
    await asyncio.sleep(1)
    mq_task.cancel()
    server_task.cancel()

    # assert
    mock_input.assert_called_once()
    mock_input.assert_called_with(
        HELP_REQUEST_TEMPLATE_STR.format(input_str="Mock human req.")
    )
    assert len(human_output_consumer.processed_messages) == 1
    assert (
        human_output_consumer.processed_messages[0].data.get("result")
        == "Test human input."
    )
    assert human_output_consumer.processed_messages[0].data.get("id_") == "1"
    assert result == {"human_request_id": req.id_}
    assert len(human_service._outstanding_human_requests) == 0