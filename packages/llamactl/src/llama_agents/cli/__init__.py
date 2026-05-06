import warnings

import llama_agents.cli.commands.agentcore  # noqa: F401
import llama_agents.cli.commands.auth  # noqa: F401
import llama_agents.cli.commands.completion  # noqa: F401
import llama_agents.cli.commands.config  # noqa: F401
import llama_agents.cli.commands.deployment  # noqa: F401
import llama_agents.cli.commands.dev  # noqa: F401
import llama_agents.cli.commands.environments  # noqa: F401
import llama_agents.cli.commands.init  # noqa: F401
import llama_agents.cli.commands.organizations  # noqa: F401
import llama_agents.cli.commands.pkg  # noqa: F401
import llama_agents.cli.commands.projects  # noqa: F401
import llama_agents.cli.commands.serve  # noqa: F401

from .app import app

# Disable warnings in llamactl CLI, and specifically silence the Pydantic
# UnsupportedFieldAttributeWarning about `validate_default` on Field().
warnings.simplefilter("ignore")
warnings.filterwarnings(
    "ignore",
    message=r"The 'validate_default' attribute .* has no effect.*",
)


# Main entry point function (called by the script)
def main() -> None:
    app()


__all__ = ["app"]


if __name__ == "__main__":
    app()
