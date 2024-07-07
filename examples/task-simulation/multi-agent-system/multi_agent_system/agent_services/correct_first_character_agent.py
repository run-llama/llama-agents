import asyncio
import uvicorn

from llama_agents import AgentService
from llama_agents.message_queues.rabbitmq import RabbitMQMessageQueue

from llama_index.core.agent import FunctionCallingAgentWorker
from llama_index.core.tools import FunctionTool
from llama_index.llms.openai import OpenAI

from multi_agent_system.utils import load_from_env
from multi_agent_system.agent_services.decorators import exponential_delay

from logging import getLogger

logger = getLogger(__name__)

message_queue_host = load_from_env("RABBITMQ_HOST")
message_queue_port = load_from_env("RABBITMQ_NODE_PORT")
message_queue_username = load_from_env("RABBITMQ_DEFAULT_USER")
message_queue_password = load_from_env("RABBITMQ_DEFAULT_PASS")
control_plane_host = load_from_env("CONTROL_PLANE_HOST")
control_plane_port = load_from_env("CONTROL_PLANE_PORT")
correct_first_character_agent_host = load_from_env("CORRECT_FIRST_CHARACTER_AGENT_HOST")
correct_first_character_agent_port = load_from_env("CORRECT_FIRST_CHARACTER_AGENT_PORT")
localhost = load_from_env("LOCALHOST")


STARTUP_RATE = 1


# create an agent
@exponential_delay(STARTUP_RATE)
def sync_correct_first_character(input: str) -> str:
    """Corrects the first character."""
    logger.info(f"received task input: {input}")
    tokens = input.split()
    res = " ".join([t[-1] + t[0:-1] for t in tokens])
    logger.info(f"Corrected first character: {res}")
    return res


@exponential_delay(STARTUP_RATE)
async def async_correct_first_character(input: str) -> str:
    """Corrects the first character."""
    logger.info(f"received task input: {input}")
    tokens = input.split()
    res = " ".join([t[-1] + t[0:-1] for t in tokens])
    logger.info(f"Corrected first character: {res}")
    return res


tool = FunctionTool.from_defaults(
    fn=sync_correct_first_character, async_fn=async_correct_first_character
)
worker = FunctionCallingAgentWorker.from_tools(
    [tool], llm=OpenAI(), max_function_calls=1
)
agent = worker.as_agent()

# create agent server
message_queue = RabbitMQMessageQueue(
    url=f"amqp://{message_queue_username}:{message_queue_password}@{message_queue_host}:{message_queue_port}/"
)

agent_server = AgentService(
    agent=agent,
    message_queue=message_queue,
    description="Brings back the last character to the correct position.",
    service_name="correct_first_character_agent",
    host=correct_first_character_agent_host,
    port=(
        int(correct_first_character_agent_port)
        if correct_first_character_agent_port
        else None
    ),
)

app = agent_server._app


# launch
async def launch() -> None:
    # register to message queue
    start_consuming_callable = await agent_server.register_to_message_queue()
    _ = asyncio.create_task(start_consuming_callable())

    # register to control plane
    await agent_server.register_to_control_plane(
        control_plane_url=(
            f"http://{control_plane_host}:{control_plane_port}"
            if control_plane_port
            else f"http://{control_plane_host}"
        )
    )

    cfg = uvicorn.Config(
        agent_server._app,
        host=localhost,
        port=agent_server.port,
    )
    server = uvicorn.Server(cfg)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(launch())
