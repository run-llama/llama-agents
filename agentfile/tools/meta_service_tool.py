import asyncio
import logging
import uuid
from typing import Any, Dict, Optional
from pydantic.v1 import Field, PrivateAttr
from llama_index.core.bridge.pydantic import BaseModel
from llama_index.core.tools import AsyncBaseTool, ToolMetadata, ToolOutput
from agentfile.messages.base import QueueMessage
from agentfile.message_consumers.base import BaseMessageQueueConsumer
from agentfile.message_consumers.callable import CallableMessageConsumer
from agentfile.message_queues.base import BaseMessageQueue
from agentfile.message_publishers.publisher import (
    MessageQueuePublisherMixin,
    PublishCallback,
)
from agentfile.services.tool import ToolService
from agentfile.types import (
    ActionTypes,
    ToolCallBundle,
    ToolCall,
    ToolCallResult,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG)


class MetaServiceTool(MessageQueuePublisherMixin, AsyncBaseTool, BaseModel):
    tool_call_results: Dict[str, ToolCallResult] = Field(default_factory=dict)
    timeout: float = Field(default=10.0, description="timeout interval in seconds.")
    tool_service_name: str = Field(default_factory=str)
    step_interval: float = 0.1
    raise_timeout: bool = False
    registered: bool = False

    _message_queue: BaseMessageQueue = PrivateAttr()
    _publisher_id: str = PrivateAttr()
    _publish_callback: Optional[PublishCallback] = PrivateAttr()
    _lock: asyncio.Lock = PrivateAttr()
    _metadata: ToolMetadata = PrivateAttr()

    def __init__(
        self,
        tool_metadata: ToolMetadata,
        message_queue: BaseMessageQueue,
        tool_service_name: str,
        publish_callback: Optional[PublishCallback] = None,
        tool_call_results: Dict[str, ToolCallResult] = {},
        timeout: float = 10.0,
        step_interval: float = 0.1,
        raise_timeout: bool = False,
    ) -> None:
        super().__init__(
            tool_call_results=tool_call_results,
            timeout=timeout,
            step_interval=step_interval,
            tool_service_name=tool_service_name,
            raise_timeout=raise_timeout,
        )
        self._message_queue = message_queue
        self._publisher_id = f"{self.__class__.__qualname__}-{uuid.uuid4()}"
        self._publish_callback = publish_callback
        self._metadata = tool_metadata
        self._lock = asyncio.Lock()

    @classmethod
    async def from_tool_service(
        cls,
        name: str,
        message_queue: BaseMessageQueue,
        tool_service: Optional[ToolService] = None,
        tool_service_url: Optional[str] = None,
        tool_service_api_key: Optional[str] = None,
        tool_service_name: Optional[str] = None,
        publish_callback: Optional[PublishCallback] = None,
        timeout: float = 10.0,
        step_interval: float = 0.1,
        raise_timeout: bool = False,
    ) -> "MetaServiceTool":
        if tool_service is not None:
            res = await tool_service.get_tool_by_name(name)
            try:
                tool_metadata = res["tool_metadata"]
            except KeyError:
                raise ValueError("tool_metadata not found.")
            return cls(
                tool_metadata=tool_metadata,
                message_queue=message_queue,
                tool_service_name=tool_service.service_name,
                publish_callback=publish_callback,
                timeout=timeout,
                step_interval=step_interval,
                raise_timeout=raise_timeout,
            )
        # TODO by requests
        # make a http request, try to parse into BaseTool
        elif (
            tool_service_url is not None
            and tool_service_api_key is not None
            and tool_service_name is not None
        ):
            return cls(
                tool_service_name=tool_service_name,
                tool_metadata=ToolMetadata("TODO"),
                message_queue=message_queue,
                publish_callback=publish_callback,
            )
        else:
            raise ValueError(
                "Please supply either a ToolService or a triplet of {tool_service_url, tool_service_api_key, tool_service_name}."
            )

    @property
    def message_queue(self) -> BaseMessageQueue:
        return self._message_queue

    @property
    def publisher_id(self) -> str:
        return self._publisher_id

    @property
    def publish_callback(self) -> Optional[PublishCallback]:
        return self._publish_callback

    @property
    def metadata(self) -> ToolMetadata:
        return self._metadata

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock

    async def process_message(self, message: QueueMessage, **kwargs: Any) -> None:
        if message.action == ActionTypes.COMPLETED_TOOL_CALL:
            tool_call_result = ToolCallResult(**message.data or {})
            async with self.lock:
                self.tool_call_results.update({tool_call_result.id_: tool_call_result})
        else:
            raise ValueError(f"Unhandled action: {message.action}")

    def as_consumer(self) -> BaseMessageQueueConsumer:
        return CallableMessageConsumer(
            message_type=self.publisher_id,
            handler=self.process_message,
        )

    async def purge_old_tool_call_results(self, cutoff_date: str) -> None:
        """Purge old tool call results.

        TODO: implement this.
        """
        pass

    async def _poll_for_tool_call_result(self, tool_call_id: str) -> ToolCallResult:
        tool_call_result = None
        while tool_call_result is None:
            async with self.lock:
                tool_call_result = (
                    self.tool_call_results[tool_call_id]
                    if tool_call_id in self.tool_call_results
                    else None
                )

            await asyncio.sleep(self.step_interval)
        return tool_call_result

    async def deregister(self) -> None:
        """Deregister from message queue."""
        await self.message_queue.deregister_consumer(self.as_consumer())
        self.registered = False

    def call(self, *args: Any, **kwargs: Any) -> ToolOutput:
        """Call."""
        return asyncio.run(self.acall(*args, **kwargs))

    async def acall(self, *args: Any, **kwargs: Any) -> ToolOutput:
        """Publish a call to the queue.

        In order to get a ToolOutput result, this will poll the queue until
        the result is written.
        """
        if not self.registered:
            # register tool to message queue
            await self.message_queue.register_consumer(self.as_consumer())
            self.registered = True

        tool_call = ToolCall(
            tool_call_bundle=ToolCallBundle(
                tool_name=self.metadata.name, tool_args=args, tool_kwargs=kwargs
            ),
            source_id=self.publisher_id,
        )
        await self.publish(
            QueueMessage(
                type=self.tool_service_name,
                action=ActionTypes.NEW_TOOL_CALL,
                data=tool_call.dict(),
            )
        )

        # poll for tool_call_result with max timeout
        try:
            tool_call_result = await asyncio.wait_for(
                self._poll_for_tool_call_result(tool_call_id=tool_call.id_),
                timeout=self.timeout,
            )
        except (
            asyncio.exceptions.TimeoutError,
            asyncio.TimeoutError,
            TimeoutError,
        ) as e:
            logger.debug(f"Timeout reached for tool_call with id {tool_call.id_}")
            if self.raise_timeout:
                raise
            return ToolOutput(
                content="Encountered error: " + str(e),
                tool_name=self.metadata.name,
                raw_input={"args": args, "kwargs": kwargs},
                raw_output=str(e),
                is_error=True,
            )
        finally:
            async with self.lock:
                if tool_call.id_ in self.tool_call_results:
                    del self.tool_call_results[tool_call.id_]

        return ToolOutput(
            content=tool_call_result.result,
            tool_name=self.metadata.name,
            raw_input={"args": args, "kwargs": kwargs},
            raw_output=tool_call_result.result,
        )
