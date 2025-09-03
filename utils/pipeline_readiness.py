import os
from loguru import logger

from const import READY_CHECK_FILEPATH


def needs_pipeline_ready(func):
    """
    Decorator that checks if the pipeline is ready before running a command.
    Exits with code 1 otherwise.
    """

    def wrapper(*args, **kwargs):
        if not check_pipeline_readiness():
            logger.error("Pipeline is not ready. Cannot run command.")
            exit(1)

        return func(*args, **kwargs)

    return wrapper


def set_pipeline_readiness(is_ready=True) -> bool:
    """
    Creates a file to indicate that the pipeline is ready to work with.
    """
    # Mark as ready
    if is_ready:
        with open(READY_CHECK_FILEPATH, "w+") as file:
            file.write("READY")

    # Mark as not ready
    if not is_ready and os.path.exists(READY_CHECK_FILEPATH):
        os.remove(READY_CHECK_FILEPATH)

    return is_ready


def check_pipeline_readiness() -> bool:
    """
    Checks whether the pipeline is ready to work with.
    Main indicator: presence of a file at `OUTPUT_READY_CHECK_FILEPATH`.
    """
    return os.path.exists(READY_CHECK_FILEPATH)
