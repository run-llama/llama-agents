import logging


def setup_file_logging(
    log_file: str = "llamactl.log", level: int = logging.DEBUG
) -> None:
    """Set up global file logging for debugging CLI commands."""
    # Configure the root logger
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode="a"),
        ],
        force=True,  # Override any existing configuration
    )
