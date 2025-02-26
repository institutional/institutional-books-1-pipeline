from pathlib import Path
import glob

import click
import peewee
import humanize

import utils
import models
from const import (
    TABLES,
    OUTPUT_DATABASE_DIR_PATH,
    OUTPUT_DATABASE_FILENAME,
    INPUT_DIR_PATH,
    INPUT_JSONL_DIR_PATH,
    INPUT_CSV_DIR_PATH,
)


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
        click.echo("✅ The pipeline is ready.")
    else:
        click.echo("🛑 The pipeline is NOT ready.")
        click.echo("-- Run `setup build` command")
        exit(0)

    #
    # Database
    #
    _print_section_heading("Database status")

    db_size = Path(OUTPUT_DATABASE_DIR_PATH, OUTPUT_DATABASE_FILENAME).stat().st_size
    print(f"Database size: {humanize.naturalsize(db_size)}")

    for table_name, model_name in TABLES.items():
        model: peewee.Model = models.__getattribute__(model_name)
        click.echo(f"Table {table_name}: {humanize.intcomma(model.select().count())} record(s)")

    #
    # Input folder size
    #
    _print_section_heading("Size of the collection")

    jsonl_total_size = 0
    csv_total_size = 0

    for filepath in glob.glob(f"{INPUT_JSONL_DIR_PATH}/*.jsonl"):
        jsonl_total_size += Path(filepath).stat().st_size

    for filepath in glob.glob(f"{INPUT_CSV_DIR_PATH}/*.csv"):
        csv_total_size += Path(filepath).stat().st_size

    click.echo(f"Total size: {humanize.naturalsize(jsonl_total_size + csv_total_size)}")
    click.echo(f"JSONL: {humanize.naturalsize(jsonl_total_size)}")
    click.echo(f"CSV: {humanize.naturalsize(csv_total_size)}")
