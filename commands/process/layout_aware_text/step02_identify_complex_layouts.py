import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import traceback
from datetime import datetime

import click

import utils
from models import BookIO, LayoutData, PageCount
from models.layout_data import (
    PageMetadata,
    OCRChunk,
    OCRChunkType,
    LayoutSeparator,
    LayoutSeparatorType,
)

MIN_AVERAGE_CHAR_LENGTH = 30
""" Average per-line character threshold under which a layout is likely to be complex. """

MAX_SEPARATORS = 5
""" If a page contains more than X detected separators, it will be considered a complex layout. """


@click.command("step02-identify-complex-layouts")
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
@click.option(
    "--max-workers",
    type=int,
    required=False,
    default=multiprocessing.cpu_count(),
    help="Determines how many subprocesses can be run in parallel.",
)
@utils.needs_pipeline_ready
def step02_identify_complex_layouts(
    offset: int | None,
    limit: int | None,
    max_workers: int,
):
    """
    Layout-aware text processing, step 02:
    Analyses the layout of each page to determine if it is likely "simple" or "complex".

    Notes:
    - Complex layout = Pages for which the OCR text is made mainly of very short lines, or in which we detected a lot of layout separators
    - Updates `LayoutData` files in place.
    """
    #
    # Data dependency checks
    #
    try:
        assert BookIO.select().count() == PageCount.select().count()
    except:
        click.echo("This command needs page count information.")
        exit(1)

    #
    # Process books in parallel
    #
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for book in BookIO.select().offset(offset).limit(limit).order_by(BookIO.barcode).iterator():
            future = executor.submit(process_book, book)

        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                click.echo(traceback.format_exc())
                click.echo("Could not detect complex layouts. Interrupting.")
                executor.shutdown(wait=False, cancel_futures=True)
                exit(1)


def process_book(book: BookIO) -> bool:
    """
    Runs layout complexity analysis on all the pages of a given book and saves results to disk.
    """
    # Check if there is a layout data file available for this book
    if not LayoutData.exists(barcode=book.barcode):
        click.echo(f"⏭️ #{book.barcode} does not have layout data available. Skipping.")
        return False

    processing_start = datetime.now()
    processing_end = None
    layout_data = LayoutData.get(barcode=book.barcode)
    total_complex = 0
    total_simple = 0

    #
    # Evaluate each page
    #
    for page in range(0, book.pagecount_set[0].count_from_ocr):
        is_complex_layout = False

        # First check: less than X characters per line in OCR'd text on average
        chars_per_line = [len(line) for line in book.text[page].split("\n")]
        average_chars_per_line = sum(chars_per_line) / len(chars_per_line)

        if average_chars_per_line < MIN_AVERAGE_CHAR_LENGTH:
            is_complex_layout = True

        # Second check: separators density
        if not is_complex_layout and len(layout_data.separators_by_page[page]) >= 5:
            is_complex_layout = True

        # Update page metadata
        layout_data.page_metadata_by_page[page].complex_layout = is_complex_layout

        if is_complex_layout:
            total_complex += 1
        else:
            total_simple += 1

    # Save layout_data object
    layout_data.save()

    processing_end = datetime.now()

    click.echo(
        f"⏱️ #{book.barcode} - "
        + f"{total_complex} complex layouts, {total_simple} simple layouts. "
        + f"Processed in {processing_end - processing_start}"
    )
