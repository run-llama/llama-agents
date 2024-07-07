import logging

from llama_agents import console_handler

root_logger = logging.getLogger("multi_agent_system")
root_logger.addHandler(console_handler)
root_logger.setLevel(logging.INFO)
