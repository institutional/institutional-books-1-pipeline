from pathlib import Path
import glob
import multiprocessing
import os

import click
import peewee
import humanize
from loguru import logger

import utils
import models
from const import DATABASE_DIR_PATH, DATABASE_FILENAME


@click.command("status")
def status():
    """
    Reports on the pipeline's status (database and cache size, etc ...)
    """

    def _print_section_heading(heading: str):
        click.echo(80 * "-")
        click.echo(heading)
        click.echo(80 * "-")

    #
    # Pipeline readiness
    #
    _print_section_heading("Pipeline status")

    if utils.check_pipeline_readiness():
        logger.info("The pipeline is ready.")
    else:
        logger.error("The pipeline is NOT ready. Run `setup build` command.")
        exit(1)

    #
    # Database
    #
    _print_section_heading("Database status")

    db_size = Path(DATABASE_DIR_PATH, DATABASE_FILENAME).stat().st_size
    print(f"Database size: {humanize.naturalsize(db_size)}")

    available_models = [model_name for model_name in dir(models) if model_name[0].isupper()]

    for model_name in available_models:
        model: peewee.Model = models.__getattribute__(model_name)
        table_name = model._meta.table_name
        click.echo(f"Table {table_name}: {humanize.intcomma(model.select().count())} record(s)")

    #
    # Resources
    #
    _print_section_heading("Resources")
    click.echo(f"Total CPU cores/threads: {multiprocessing.cpu_count()}")
    click.echo(f"Torch Devices: {", ".join(utils.get_torch_devices())}")

    #
    # Cache
    #
    _print_section_heading("Cache status")

    cache_size_max = int(os.getenv("CACHE_MAX_SIZE_IN_GB", 1)) * 1_000_000_000
    cache_size_current = utils.get_cache().volume()

    click.echo(
        f"Cache size: "
        + f"{humanize.naturalsize(cache_size_current)} / {humanize.naturalsize(cache_size_max)}"
    )
