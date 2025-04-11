import os
import traceback
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import traceback

import click
import tiktoken

import utils
from models import BookIO, MainLanguage, HathitrustRightsDetermination


@click.command("generate-layout-aware-text")
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="If set, overwrites existing entries.",
)
@click.option(
    "--offset",
    type=int,
    required=False,
    help="If set, allows for processing a subset of the collection (sorted by BookIO.barcode).",
)
@click.option(
    "--limit",
    type=int,
    required=False,
    help="If set, allows for processing a subset of the collection (sorted by BookIO.barcode).",
)
@utils.needs_pipeline_ready
def generate_layout_aware_text(
    overwrite: bool,
    offset: int | None,
    limit: int | None,
):
    """
    TODO
    """
    #
    # Data dependency checks
    #

    # Language detection in `main_language`
    try:
        assert BookIO.select().count() == MainLanguage.select().count()
        assert (
            MainLanguage.select().where(MainLanguage.from_detection_iso693_3.is_null(False)).count()
        )
    except:
        click.echo("This command needs language detection data. See `run-language-detection`.")
        exit(1)

    # Hathitrust rights determination data
    try:
        assert BookIO.select().count() == HathitrustRightsDetermination.select().count()
    except:
        click.echo(
            "This command needs Hathitrust rights determination data."
            + "See `extract-hathiturst-rights-determination`."
        )
        exit(1)

    #
    # Process books
    #
    for book in BookIO.select().offset(offset).limit(limit).order_by(BookIO.barcode).iterator():
        pass
