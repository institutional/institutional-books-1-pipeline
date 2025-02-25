import os
import glob
import click

from const import OUTPUT_PIPELINE_READY_FILEPATH


def needs_pipeline_ready(func):
    """
    Click decorator that checks if the pipeline is ready before running a command.
    Exits with code 1 otherwise.
    """

    def wrapper(*args, **kwargs):
        if not check_pipeline_readiness():
            click.echo("Pipeline is not ready. Cannot run command.")
            exit(1)
        return func(*args, **kwargs)

    return wrapper


def set_pipeline_readiness(is_ready=True) -> bool:
    """
    Creates a file to indicate that the pipeline is ready to work with.
    """
    # Mark as ready
    if is_ready:
        with open(OUTPUT_PIPELINE_READY_FILEPATH, "w+") as file:
            file.write("READY")

    # Mark as not ready
    if not is_ready and os.path.exists(OUTPUT_PIPELINE_READY_FILEPATH):
        os.remove(OUTPUT_PIPELINE_READY_FILEPATH)

    return is_ready


def check_pipeline_readiness() -> bool:
    """
    Checks whether the pipeline is ready to work with.
    Main indicator: presence of a file at `OUTPUT_OUTPUT_PIPELINE_READY_FILEPATH`.
    """
    return os.path.exists(OUTPUT_PIPELINE_READY_FILEPATH)
