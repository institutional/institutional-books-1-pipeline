from shutil import rmtree

import click

import utils
from const import (
    INPUT_DIR_PATH,
    OUTPUT_DATABASE_DIR_PATH,
    OUTPUT_EXPORT_DIR_PATH,
    OUTPUT_MISC_DIR_PATH,
)


@click.command("clear")
def clear():
    """
    Clears local data. Asks for confirmation before deleting each top-level folder/item.
    """
    anything_was_deleted = False

    if click.confirm("Delete local database"):
        rmtree(OUTPUT_DATABASE_DIR_PATH)
        anything_was_deleted = True
        click.echo(f"✅ {OUTPUT_DATABASE_DIR_PATH} was cleared")

    if click.confirm("Delete exported data"):
        rmtree(OUTPUT_EXPORT_DIR_PATH)
        anything_was_deleted = True
        click.echo(f"✅ {OUTPUT_EXPORT_DIR_PATH} was cleared")

    if click.confirm("Delete misc output data"):
        rmtree(OUTPUT_MISC_DIR_PATH)
        anything_was_deleted = True
        click.echo(f"✅ {OUTPUT_MISC_DIR_PATH} was cleared")

    if click.confirm("Delete local copy of the collection"):
        rmtree(INPUT_DIR_PATH)
        anything_was_deleted = True
        click.echo(f"✅ {INPUT_DIR_PATH} was cleared")

    # Mark pipeline as not ready if anything was deleted
    if anything_was_deleted:
        utils.set_pipeline_readiness(False)
