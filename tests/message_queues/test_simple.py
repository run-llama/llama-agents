import pytest
from typing import Any
from agentfile.message_consumers.base import BaseMessageQueueConsumer
from agentfile.message_queues.simple import SimpleMessageQueue
from agentfile.messages.base import BaseMessage


class MockMessageConsumer(BaseMessageQueueConsumer):
    async def _process_message(self, message: BaseMessage, **kwargs: Any) -> Any:
        print(f"Processed: {message.class_name()}")


class MockMessage(BaseMessage):
    @classmethod
    def class_name(cls) -> str:
        return "MockMessage"


@pytest.mark.asyncio()
async def test_simple_register_consumer() -> None:
    # Arrange
    consumer_one = MockMessageConsumer()
    consumer_two = MockMessageConsumer(message_type=MockMessage)
    mq = SimpleMessageQueue()

    # Act
    await mq.register_consumer(consumer_one)
    await mq.register_consumer(consumer_two)
    with pytest.raises(ValueError):
        await mq.register_consumer(consumer_two)

    # Assert
    assert consumer_one.id_ in [
        c.id_ for c in await mq.get_consumers(consumer_one.message_type)
    ]
    assert consumer_two.id_ in [
        c.id_ for c in await mq.get_consumers(consumer_two.message_type)
    ]


@pytest.mark.asyncio()
async def test_simple_deregister_consumer() -> None:
    # Arrange
    consumer_one = MockMessageConsumer()
    consumer_two = MockMessageConsumer(message_type=MockMessage)
    consumer_three = MockMessageConsumer(message_type=MockMessage)
    mq = SimpleMessageQueue()

    await mq.register_consumer(consumer_one)
    await mq.register_consumer(consumer_two)
    await mq.register_consumer(consumer_three)

    # Act
    await mq.deregister_consumer(consumer_one)
    await mq.deregister_consumer(consumer_three)
    with pytest.raises(ValueError):
        await mq.deregister_consumer(consumer_three)

    # Assert
    assert len(await mq.get_consumers(MockMessage)) == 1
    assert len(await mq.get_consumers(BaseMessage)) == 0
