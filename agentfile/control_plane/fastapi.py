import uuid
import uvicorn
from fastapi import FastAPI
from typing import Any, Callable, Dict, List, Optional

from llama_index.core import StorageContext, VectorStoreIndex
from llama_index.core.objects import ObjectIndex, SimpleObjectNodeMapping
from llama_index.core.storage.kvstore.types import BaseKVStore
from llama_index.core.storage.kvstore import SimpleKVStore
from llama_index.core.vector_stores.types import BasePydanticVectorStore

from agentfile.control_plane.base import BaseControlPlane
from agentfile.message_consumers.base import BaseMessageQueueConsumer
from agentfile.message_queues.base import BaseMessageQueue, PublishCallback
from agentfile.messages.base import QueueMessage
from agentfile.orchestrators.base import BaseOrchestrator
from agentfile.orchestrators.service_tool import ServiceTool
from agentfile.types import (
    ActionTypes,
    ServiceDefinition,
    TaskDefinition,
    TaskResult,
)

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO)


class ControlPlaneMessageConsumer(BaseMessageQueueConsumer):
    message_handler: Dict[str, Callable]
    message_type: str = "control_plane"

    async def _process_message(self, message: QueueMessage, **kwargs: Any) -> None:
        action = message.action
        if action not in self.message_handler:
            raise ValueError(f"Action {action} not supported by control plane")

        if action == ActionTypes.NEW_TASK and message.data is not None:
            await self.message_handler[action](TaskDefinition(**message.data))
        elif action == ActionTypes.COMPLETED_TASK and message.data is not None:
            await self.message_handler[action](TaskResult(**message.data))


class FastAPIControlPlane(BaseControlPlane):
    def __init__(
        self,
        message_queue: BaseMessageQueue,
        orchestrator: BaseOrchestrator,
        vector_store: Optional[BasePydanticVectorStore] = None,
        publish_callback: Optional[PublishCallback] = None,
        state_store: Optional[BaseKVStore] = None,
        services_store_key: str = "services",
        tasks_store_key: str = "tasks",
        step_interval: float = 0.1,
        services_retrieval_threshold: int = 5,
        running: bool = True,
    ) -> None:
        self.orchestrator = orchestrator
        self.object_index = ObjectIndex(
            VectorStoreIndex(
                nodes=[],
                storage_context=StorageContext.from_defaults(vector_store=vector_store),
            ),
            SimpleObjectNodeMapping(),
        )
        self.step_interval = step_interval
        self.running = running

        self.state_store = state_store or SimpleKVStore()

        # TODO: should we store services in a tool retriever?
        self.services_store_key = services_store_key
        self.tasks_store_key = tasks_store_key

        self._message_queue = message_queue
        self._publisher_id = f"{self.__class__.__qualname__}-{uuid.uuid4()}"
        self._publish_callback = publish_callback

        self._services_cache: Dict[str, ServiceDefinition] = {}
        self._total_services = 0
        self._services_retrieval_threshold = services_retrieval_threshold

        self.app = FastAPI()
        self.app.add_api_route("/", self.home, methods=["GET"], tags=["Control Plane"])

        self.app.add_api_route(
            "/services/register",
            self.register_service,
            methods=["POST"],
            tags=["Services"],
        )
        self.app.add_api_route(
            "/services/deregister",
            self.deregister_service,
            methods=["POST"],
            tags=["Services"],
        )

        self.app.add_api_route(
            "/tasks", self.create_task, methods=["POST"], tags=["Tasks"]
        )
        self.app.add_api_route(
            "/tasks/{task_id}", self.get_task_state, methods=["GET"], tags=["Tasks"]
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

    def as_consumer(self) -> BaseMessageQueueConsumer:
        return ControlPlaneMessageConsumer(
            message_handler={
                ActionTypes.NEW_TASK: self.create_task,
                ActionTypes.COMPLETED_TASK: self.handle_service_completion,
            }
        )

    def launch(self) -> None:
        uvicorn.run(self.app)

    async def home(self) -> Dict[str, str]:
        return {
            "running": str(self.running),
            "step_interval": str(self.step_interval),
            "services_store_key": self.services_store_key,
            "total_services": str(self._total_services),
            "services_retrieval_threshold": str(self._services_retrieval_threshold),
        }

    async def register_service(self, service_def: ServiceDefinition) -> None:
        await self.state_store.aput(
            service_def.service_name,
            service_def.dict(),
            collection=self.services_store_key,
        )

        # decide to use cache vs. retrieval
        self._total_services += 1
        if self._total_services > self._services_retrieval_threshold:
            # TODO: currently blocking, should be async
            self.object_index.insert_object(service_def.dict())
            for service in self._services_cache.values():
                self.object_index.insert_object(service.dict())
            self._services_cache = {}
        else:
            self._services_cache[service_def.service_name] = service_def

    async def deregister_service(self, service_name: str) -> None:
        deleted = await self.state_store.adelete(
            service_name, collection=self.services_store_key
        )
        if service_name in self._services_cache:
            del self._services_cache[service_name]

        if deleted:
            self._total_services -= 1
        # TODO: object index does not have delete yet

    async def create_task(self, task_def: TaskDefinition) -> None:
        await self.state_store.aput(
            task_def.task_id, task_def.dict(), collection=self.tasks_store_key
        )

        task_def = await self.send_task_to_service(task_def)
        await self.state_store.aput(
            task_def.task_id, task_def.dict(), collection=self.tasks_store_key
        )

    async def send_task_to_service(self, task_def: TaskDefinition) -> TaskDefinition:
        if self._total_services > self._services_retrieval_threshold:
            service_retriever = self.object_index.as_retriever(similarity_top_k=5)

            # could also route based on similarity alone.
            # TODO: Figure out user-specified routing
            service_def_dicts: List[dict] = await service_retriever.aretrieve(
                task_def.input
            )
            service_defs = [
                ServiceDefinition.parse_obj(service_def_dict)
                for service_def_dict in service_def_dicts
            ]
        else:
            service_defs = list(self._services_cache.values())

        service_tools = [
            ServiceTool.from_service_definition(service_def)
            for service_def in service_defs
        ]
        next_messages, task_state = await self.orchestrator.get_next_messages(
            task_def, service_tools, task_def.state
        )

        for message in next_messages:
            await self.publish(message)

        task_def.state.update(task_state)
        return task_def

    async def handle_service_completion(
        self,
        task_result: TaskResult,
    ) -> None:
        # add result to task state
        task_def = await self.get_task_state(task_result.task_id)
        state = await self.orchestrator.add_result_to_state(task_result, task_def.state)
        task_def.state.update(state)

        # generate and send new tasks (if any)
        task_def = await self.send_task_to_service(task_def)

        await self.state_store.aput(
            task_def.task_id, task_def.dict(), collection=self.tasks_store_key
        )

    async def get_task_state(self, task_id: str) -> TaskDefinition:
        state_dict = await self.state_store.aget(
            task_id, collection=self.tasks_store_key
        )
        if state_dict is None:
            raise ValueError(f"Task with id {task_id} not found")

        return TaskDefinition.parse_obj(state_dict)

    async def get_all_tasks(self) -> Dict[str, TaskDefinition]:
        state_dicts = await self.state_store.aget_all(collection=self.tasks_store_key)
        return {
            task_id: TaskDefinition.parse_obj(state_dict)
            for task_id, state_dict in state_dicts.items()
        }
