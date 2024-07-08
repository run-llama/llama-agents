from abc import ABC, abstractmethod
from typing import Any, Optional
from llama_agents.messages.base import QueueMessage
from llama_agents.message_queues.base import BaseMessageQueue, PublishCallback
from llama_agents.types import TASK_CONSUMER_NAME, TaskDefinition, TaskResult


class MessageQueuePublisherMixin(ABC):
    """PublisherMixin.

    Mixin for a message queue publisher. Allows for accessing common properties and methods for:
    - Publisher ID.
    - Message queue.
    - Publish callback.
    - Publish method.
    """

    @property
    @abstractmethod
    def publisher_id(self) -> str:
        ...

    @property
    @abstractmethod
    def message_queue(self) -> BaseMessageQueue:
        ...

    @property
    def publish_callback(self) -> Optional[PublishCallback]:
        return None

    async def publish(self, message: QueueMessage, **kwargs: Any) -> Any:
        """Publish message."""
        # publish to task consumer queue if exists
        try:
            task_def = TaskDefinition(**message.data)
        except Exception:
            task_def = None

        try:
            task_res = TaskResult(**message.data)
        except Exception:
            task_res = None

        if task_def or task_res:
            message_copy = message.model_copy(deep=True)
            message_copy.type = TASK_CONSUMER_NAME
            message_copy.data.update({"original_type": message.type})
            _ = await self.message_queue.publish(message_copy)

        message.publisher_id = self.publisher_id
        return await self.message_queue.publish(
            message, callback=self.publish_callback, **kwargs
        )
