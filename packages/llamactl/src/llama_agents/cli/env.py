"""Environment variable handling utilities for llamactl"""

from io import StringIO
from typing import Dict

from dotenv import dotenv_values
from llama_agents.cli.output import warning


def load_env_secrets_from_string(env_content: str) -> Dict[str, str]:
    """
    Load environment variables from string content to use as secrets.

    Args:
        env_content: String content containing environment variables in .env format

    Returns:
        Dictionary of environment variable names and values
    """
    try:
        # Use StringIO to create a file-like object from the string
        # dotenv_values can parse from a stream
        env_stream = StringIO(env_content)
        secrets = dotenv_values(stream=env_stream)
        # Filter out None values and convert to strings
        return {k: str(v) for k, v in secrets.items() if v is not None}
    except Exception as e:
        warning(f"could not parse environment variables from string: {e}")
        return {}
