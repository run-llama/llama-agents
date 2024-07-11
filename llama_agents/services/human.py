import asyncio
import uuid
import uvicorn
from asyncio import Lock
from fastapi import FastAPI
from logging import getLogger
from pydantic import ConfigDict, PrivateAttr
from typing import Any, cast, Dict, List, Optional, Protocol, runtime_checkable

from llama_index.core.llms import MessageRole
from llama_index.core.prompts import PromptTemplate
from llama_index.core.prompts.mixin import PromptMixin, PromptDictType, PromptMixinType

from llama_agents.message_consumers.base import BaseMessageQueueConsumer
from llama_agents.message_consumers.callable import CallableMessageConsumer
from llama_agents.message_consumers.remote import RemoteMessageConsumer
from llama_agents.message_publishers.publisher import PublishCallback
from llama_agents.message_queues.base import BaseMessageQueue
from llama_agents.messages.base import QueueMessage
from llama_agents.services.base import BaseService
from llama_agents.types import (
    ActionTypes,
    ChatMessage,
    HumanResponse,
    TaskDefinition,
    TaskResult,
    ServiceDefinition,
    CONTROL_PLANE_NAME,
)


logger = getLogger(__name__)


HELP_REQUEST_TEMPLATE_STR = (
    "Your assistance is needed. Please respond to the request "
    "provided below:\n===\n\n"
    "{input_str}\n\n===\n"
)

default_human_input_prompt_template = PromptTemplate(HELP_REQUEST_TEMPLATE_STR)


@runtime_checkable
class HumanInputFn(Protocol):
    """Protocol for getting human input."""

    def __call__(self, prompt: str, **kwargs: Any) -> str:
        ...


def default_human_input_fn(prompt: str, **kwargs: Any) -> str:
    return input(prompt)


class HumanService(PromptMixin, BaseService):
    """A human service for providing human-in-the-loop assistance.

    When launched locally, it will prompt the user for input, which is blocking!

    When launched as a server, it will provide an API for creating and handling tasks.

    Exposes the following endpoints:
    - GET `/`: Get the service information.
    - POST `/process_message`: Process a message.
    - POST `/tasks`: Create a task.
    - GET `/tasks`: Get all tasks.
    - GET `/tasks/{task_id}`: Get a task.
    - POST `/tasks/{task_id}/handle`: Handle a task.

    Attributes:
        service_name (str): The name of the service.
        description (str): The description of the service.
        running (bool): Whether the service is running.
        step_interval (float): The interval in seconds to poll for tool call results. Defaults to 0.1s.
        host (Optional[str]): The host of the service.
        port (Optional[int]): The port of the service.


    """

    model_config = ConfigDict(arbitrary_types_allowed=True)
    service_name: str
    description: str = "Local Human Service."
    running: bool = True
    step_interval: float = 0.1
    fn_input: HumanInputFn = default_human_input_fn
    human_input_prompt: PromptTemplate = default_human_input_prompt_template
    host: Optional[str] = None
    port: Optional[int] = None

    _outstanding_human_tasks: List[TaskDefinition] = PrivateAttr()
    _message_queue: BaseMessageQueue = PrivateAttr()
    _app: FastAPI = PrivateAttr()
    _publisher_id: str = PrivateAttr()
    _publish_callback: Optional[PublishCallback] = PrivateAttr()
    _lock: Lock = PrivateAttr()

    def __init__(
        self,
        message_queue: BaseMessageQueue,
        running: bool = True,
        description: str = "Local Human Service",
        service_name: str = "default_human_service",
        publish_callback: Optional[PublishCallback] = None,
        step_interval: float = 0.1,
        fn_input: HumanInputFn = default_human_input_fn,
        human_input_prompt: PromptTemplate = default_human_input_prompt_template,
        host: Optional[str] = None,
        port: Optional[int] = None,
    ) -> None:
        super().__init__(
            running=running,
            description=description,
            service_name=service_name,
            step_interval=step_interval,
            fn_input=fn_input,
            human_input_prompt=human_input_prompt,
            host=host,
            port=port,
        )

        self._outstanding_human_tasks = []
        self._message_queue = message_queue
        self._publisher_id = f"{self.__class__.__qualname__}-{uuid.uuid4()}"
        self._publish_callback = publish_callback
        self._lock = asyncio.Lock()
        self._app = FastAPI()

        self._app.add_api_route("/", self.home, methods=["GET"], tags=["Human Service"])
        self._app.add_api_route(
            "/process_message",
            self.process_message,
            methods=["POST"],
            tags=["Human Service"],
        )

        self._app.add_api_route(
            "/tasks", self.create_task, methods=["POST"], tags=["Tasks"]
        )
        self._app.add_api_route(
            "/tasks", self.get_tasks, methods=["GET"], tags=["Tasks"]
        )
        self._app.add_api_route(
            "/tasks/{task_id}", self.get_task, methods=["GET"], tags=["Tasks"]
        )
        self._app.add_api_route(
            "/tasks/{task_id}/handle",
            self.handle_task,
            methods=["POST"],
            tags=["Tasks"],
        )

    @property
    def service_definition(self) -> ServiceDefinition:
        """Get the service definition."""
        return ServiceDefinition(
            service_name=self.service_name,
            description=self.description,
            prompt=[],
            host=self.host,
            port=self.port,
        )

    @property
    def message_queue(self) -> BaseMessageQueue:
        """The message queue."""
        return self._message_queue

    @property
    def publisher_id(self) -> str:
        """The publisher ID."""
        return self._publisher_id

    @property
    def publish_callback(self) -> Optional[PublishCallback]:
        """The publish callback, if any."""
        return self._publish_callback

    @property
    def lock(self) -> Lock:
        return self._lock

    async def processing_loop(self) -> None:
        """The processing loop for the service."""
        while True:
            if not self.running:
                await asyncio.sleep(self.step_interval)
                continue

            async with self.lock:
                try:
                    task_def = self._outstanding_human_tasks.pop(0)
                except IndexError:
                    await asyncio.sleep(self.step_interval)
                    continue

                logger.info(
                    f"Processing request for human help for task: {task_def.task_id}"
                )

                # process req
                prompt = (
                    self.human_input_prompt.format(input_str=task_def.input)
                    if self.human_input_prompt
                    else task_def.input
                )
                result = self.fn_input(prompt=prompt)

                # create history
                history = [
                    ChatMessage(
                        role=MessageRole.ASSISTANT,
                        content=HELP_REQUEST_TEMPLATE_STR.format(
                            input_str=task_def.input
                        ),
                    ),
                    ChatMessage(role=MessageRole.USER, content=result),
                ]

                # publish the completed task
                await self.publish(
                    QueueMessage(
                        type=CONTROL_PLANE_NAME,
                        action=ActionTypes.COMPLETED_TASK,
                        data=TaskResult(
                            task_id=task_def.task_id,
                            history=history,
                            result=result,
                        ).model_dump(),
                    )
                )

            await asyncio.sleep(self.step_interval)

    async def process_message(self, message: QueueMessage) -> None:
        """Process a message received from the message queue."""
        if message.action == ActionTypes.NEW_TASK:
            task_def = TaskDefinition(**message.data or {})
            async with self.lock:
                self._outstanding_human_tasks.append(task_def)
        else:
            raise ValueError(f"Unhandled action: {message.action}")

    def as_consumer(self, remote: bool = False) -> BaseMessageQueueConsumer:
        """Get the consumer for the service.

        Args:
            remote (bool):
                Whether the consumer is remote. Defaults to False.
                If True, the consumer will be a RemoteMessageConsumer that uses the `process_message` endpoint.
        """
        if remote:
            url = (
                f"http://{self.host}:{self.port}{self._app.url_path_for('process_message')}"
                if self.port
                else f"http://{self.host}{self._app.url_path_for('process_message')}"
            )
            return RemoteMessageConsumer(
                id_=self.publisher_id,
                url=url,
                message_type=self.service_name,
            )

        return CallableMessageConsumer(
            id_=self.publisher_id,
            message_type=self.service_name,
            handler=self.process_message,
        )

    async def launch_local(self) -> asyncio.Task:
        """Launch the service in-process."""
        logger.info(f"{self.service_name} launch_local")
        return asyncio.create_task(self.processing_loop())

    # ---- Server based methods ----

    async def home(self) -> Dict[str, str]:
        """Get general service information."""
        return {
            "service_name": self.service_name,
            "description": self.description,
            "running": str(self.running),
            "step_interval": str(self.step_interval),
            "num_tasks": str(len(self._outstanding_human_tasks)),
            "tasks": "\n".join([str(task) for task in self._outstanding_human_tasks]),
            "type": "human_service",
        }

    async def create_task(self, task: TaskDefinition) -> Dict[str, str]:
        """Create a task for the human service."""
        async with self.lock:
            self._outstanding_human_tasks.append(task)
        return {"task_id": task.task_id}

    async def get_tasks(self) -> List[TaskDefinition]:
        """Get all outstanding tasks."""
        async with self.lock:
            return [*self._outstanding_human_tasks]

    async def get_task(self, task_id: str) -> Optional[TaskDefinition]:
        """Get a specific task by ID."""
        async with self.lock:
            for task in self._outstanding_human_tasks:
                if task.task_id == task_id:
                    return task
        return None

    async def handle_task(self, task_id: str, result: HumanResponse) -> None:
        """Handle a task by providing a result."""
        async with self.lock:
            for task_def in self._outstanding_human_tasks:
                if task_def.task_id == task_id:
                    self._outstanding_human_tasks.remove(task_def)
                    break

        logger.info(f"Processing request for human help for task: {task_def.task_id}")

        # create history
        history = [
            ChatMessage(
                role=MessageRole.ASSISTANT,
                content=HELP_REQUEST_TEMPLATE_STR.format(input_str=task_def.input),
            ),
            ChatMessage(role=MessageRole.USER, content=result.result),
        ]

        # publish the completed task
        await self.publish(
            QueueMessage(
                type=CONTROL_PLANE_NAME,
                action=ActionTypes.COMPLETED_TASK,
                data=TaskResult(
                    task_id=task_def.task_id,
                    history=history,
                    result=result.result,
                ).model_dump(),
            )
        )

    async def launch_server(self) -> None:
        """Launch the service as a FastAPI server."""
        logger.info(
            f"Lanching server for {self.service_name} at {self.host}:{self.port}"
        )

        class CustomServer(uvicorn.Server):
            def install_signal_handlers(self) -> None:
                pass

        cfg = uvicorn.Config(self._app, host=self.host, port=self.port)
        server = CustomServer(cfg)
        await server.serve()

    def _get_prompts(self) -> PromptDictType:
        """Get prompts."""
        return {"human_input_prompt": self.human_input_prompt}

    def _get_prompt_modules(self) -> PromptMixinType:
        """Get prompt sub-modules.

        Return a dictionary of sub-modules within the current module
        that also implement PromptMixin (so that their prompts can also be get/set).

        Can be blank if no sub-modules.

        """
        return {}

    def _update_prompts(self, prompts_dict: PromptDictType) -> None:
        """Update prompts."""
        if "human_input_prompt" in prompts_dict:
            new_prompt = cast(PromptTemplate, prompts_dict["human_input_prompt"])
            self.human_input_prompt = new_prompt
