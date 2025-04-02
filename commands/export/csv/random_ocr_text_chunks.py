import csv
from pathlib import Path
import random

import click
import peewee

import utils
from models import BookIO, MainLanguage, PageCount
from const import OUTPUT_EXPORT_DIR_PATH, DATETIME_SLUG


@click.command("random-ocr-text-chunks")
@click.option(
    "--n-samples",
    type=int,
    required=False,
    default=1000,
    help="Determines how many random pages to pick and export. Total of exported samples will differ.",
)
@click.option(
    "--lang",
    default=None,
    type=str,
    help="ISO639-3 code of the main language to target.",
)
@click.option(
    "--pd-only",
    is_flag=True,
    default=True,
    help="If set, will only focus on books detected as PD.",
)
@utils.needs_pipeline_ready
def random_ocr_text_chunks(
    n_samples: int,
    lang: str | None,
    pd_only: bool,
):
    """ """
    #
    # Dependencies check
    #
    try:
        assert BookIO.select().count() == PageCount.select().count()
    except:
        click.echo("Page count data is not available.")
        exit(1)

    try:
        assert BookIO.select().count() == MainLanguage.select().count()

        assert (
            MainLanguage.select()
            .where(
                MainLanguage.from_detection_iso693_3.is_null(False),
            )
            .count()
        )
    except:
        click.echo("Language detection data is not available.")
        exit(1)

    if pd_only == True:
        raise NotImplementedError("PD ONLY FILTER NOT IMPLEMENTED")

    books = []
    pages = []

    #
    # Pick random books, filtered
    #
    for book in BookIO.select().order_by(peewee.fn.Random()).iterator():
        # Stop if we have enough samples
        if len(books) >= n_samples:
            break

        # Check that this book has text
        if book.pagecount_set[0].count_from_ocr < 10:
            break

        # Check language
        if lang and book.mainlanguage_set[0].from_detection != lang:
            continue

        # Check PD status
        if pd_only:
            pass

        books.append(book)

    #
    # Pick random page in each book
    #
    for book in books:
        # Eliminate empty pages
        # Pick a random page in the list
        pass

    #
    # Chunk the pages that were picked
    #

    #
    # Export individual chunks in a CSV
    #
