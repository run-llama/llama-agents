from llama_agents import ServerLauncher

from multi_agent_system.core_services.message_queue import message_queue
from multi_agent_system.core_services.control_plane import control_plane
from multi_agent_system.agent_services.remove_ay_agent import (
    agent_server as remove_ay_agent_server,
)
from multi_agent_system.agent_services.correct_first_character_agent import (
    agent_server as correct_first_character_agent_server,
)
from multi_agent_system.additional_services.human_consumer import (
    human_consumer_server,
)


# launch it
launcher = ServerLauncher(
    [remove_ay_agent_server, correct_first_character_agent_server],
    control_plane,
    message_queue,
    additional_consumers=[human_consumer_server.as_consumer()],
)


if __name__ == "__main__":
    launcher.launch_servers()
