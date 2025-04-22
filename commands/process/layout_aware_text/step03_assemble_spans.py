# TODO: Account for undetected separators
# TODO: Alternative merging algorithm based on sentence detection?
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
    merge_ocrchunks,
)

WORDS_HORIZONTAL_ALIGNMENT_TOLERANCE = 0.5
""" 
    Tolerance applied when assessing whether two word bounding boxes are horizontally aligned. 
    (Percentage of average height).
"""


@click.command("step03-assemble-spans")
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
def step03_assemble_spans(
    offset: int | None,
    limit: int | None,
    max_workers: int,
):
    """
    Layout-aware text processing, step 03:
    Processes layout data to loosely assemble words into spans based on their bounding boxes and detected layout separators.

    Notes:
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
                click.echo("Could not assemble spans in LayoutData objects. Interrupting.")
                executor.shutdown(wait=False, cancel_futures=True)
                exit(1)


def process_book(book: BookIO) -> bool:
    """
    Tries to assemble spans for each page of a given book
    """
    # Check if there is a layout data file available for this book
    if not LayoutData.exists(barcode=book.barcode):
        click.echo(f"⏭️ #{book.barcode} does not have layout data available. Skipping.")
        return False

    processing_start = datetime.now()
    processing_end = None
    total_words = 0
    total_spans = 0
    page_count = book.pagecount_set[0].count_from_ocr
    layout_data = LayoutData.get(barcode=book.barcode)

    # We need to have pre-processed words and separators
    if layout_data.words_by_page is None or layout_data.separators_by_page is None:
        click.echo(f"⏭️ #{book.barcode} is missing words or separators data from step02. Skipping.")
        return False

    #
    # Process spans for each page
    #
    layout_data.spans_by_page = [[] for i in range(0, page_count)]

    for page in range(0, page_count):
        spans = []
        vertical_separators = []
        words = layout_data.words_by_page[page]

        # Skip if there are no words to process
        if not words:
            continue

        # Grab vertical separators
        vertical_separators = [
            separator
            for separator in layout_data.separators_by_page[page]
            if separator.type == LayoutSeparatorType.VERTICAL
        ]

        # Group words into spans
        current_span = [words[0]]

        for i in range(1, len(words)):
            word1, word2 = words[i - 1], words[i]

            words_are_on_same_line = check_words_horizontal_alignment(word1, word2)

            words_are_split_by_vsep = check_words_for_vertical_split(
                vertical_separators,
                word1,
                word2,
            )

            if words_are_on_same_line and not words_are_split_by_vsep:
                current_span.append(word2)
            else:
                spans.append(merge_ocrchunks(current_span, OCRChunkType.SPAN))
                current_span = [word2]

        # Add the last span
        if current_span:
            spans.append(merge_ocrchunks(current_span, OCRChunkType.SPAN))

        # Update layout data object
        layout_data.spans_by_page[page] = spans
        total_spans += len(spans)
        total_words += len(words)

    # Save layout_data object
    layout_data.save()

    processing_end = datetime.now()

    click.echo(
        f"⏱️ #{book.barcode} - "
        + f"{total_spans} spans assembled from {total_words} words. "
        + f"Processed in {processing_end - processing_start}"
    )


def check_words_horizontal_alignment(word1: OCRChunk, word2: OCRChunk) -> bool:
    """
    Returns True if two words are (roughly) on the same horizontal line.
    Assessment based on their respective bounding boxes + tolerance.
    """
    overlap = min(word1.y_max, word2.y_max) - max(word1.y_min, word2.y_min)
    avg_height = ((word1.y_max - word1.y_min) + (word2.y_max - word2.y_min)) / 2
    return overlap >= WORDS_HORIZONTAL_ALIGNMENT_TOLERANCE * avg_height


def check_words_for_vertical_split(
    vertical_separators: list[LayoutSeparator],
    word1: OCRChunk,
    word2: OCRChunk,
) -> bool:
    """
    Check for the presence of a vertical separator at the intersection of two words.
    """
    word1_y_range = range(word1.y_min, word1.y_max)
    word2_y_range = range(word2.y_min, word2.y_max)

    leftmost_right_edge = min(word1.x_min, word2.x_max)
    rightmost_left_edge = max(word1.x_max, word2.x_min)

    for separator in vertical_separators:
        # Skip if separator is not in-between both words
        if separator.x_max < leftmost_right_edge or separator.x_min > rightmost_left_edge:
            continue

        # Check intersection of vertical separator within horizontal line
        line_y_range = range(separator.y_min, separator.y_max)

        if set(line_y_range) & set(word1_y_range) and set(line_y_range) & set(word2_y_range):
            return True

    return False
