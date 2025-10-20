from shutil import rmtree

import click
from loguru import logger

import utils
from const import (
    DATABASE_DIR_PATH,
    EXPORT_DIR_PATH,
    MISC_DIR_PATH,
)


@click.command("clear")
def clear():
    """
    Clears local data. Asks for confirmation before deleting each top-level folder/item.
    """
    anything_was_deleted = False

    if click.confirm("Delete local database"):
        rmtree(DATABASE_DIR_PATH)
        anything_was_deleted = True
        logger.info(f"{DATABASE_DIR_PATH} was cleared")

    if click.confirm(f"Delete exported data ({EXPORT_DIR_PATH})"):
        rmtree(EXPORT_DIR_PATH)
        anything_was_deleted = True
        logger.info(f"{EXPORT_DIR_PATH} was cleared")

    if click.confirm(f"Delete misc output data ({MISC_DIR_PATH})"):
        rmtree(MISC_DIR_PATH)
        anything_was_deleted = True
        logger.info(f"{MISC_DIR_PATH} was cleared")

    if click.confirm("Clear disk cache"):
        with utils.get_cache() as cache:
            cache.clear()
        logger.info(f"Disk cache was cleared")

    # Mark pipeline as not ready if anything was deleted
    if anything_was_deleted:
        utils.set_pipeline_readiness(False)
