import traceback
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed

import click
from simhash import Simhash

from utils import (
    needs_pipeline_ready,
    get_batch_max_size,
    get_simhash_features,
    process_db_write_batch,
)
from models import BookIO, ScannedTextSimhash
from const import DEFAULT_SIMHASH_SHINGLE_WIDTH

SIMHASH_SHORT_SHINGLE_WIDTH_LANGUAGE = ["chi", "zho", "jpn", "tha", "lao", "bur", "khm", "tib"]
""" ISO693-2B codes of languages that require shorter shingle widths for Simhash. """


@click.command("step01-get-simhash")
@click.option(
    "--simhash-shingle-width",
    type=int,
    required=False,
    default=DEFAULT_SIMHASH_SHINGLE_WIDTH,
    help="Determines the size of the shingles used by simhash to measure similarity between texts.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="If set, overwrites existing entries.",
)
@click.option(
    "--start",
    type=int,
    required=False,
    help="If set, allows for processing a subset of the whole issues batch (sorted by BookIO.barcode).",
)
@click.option(
    "--end",
    type=int,
    required=False,
    help="If set, allows for processing a subset of the whole issues batch (sorted by BookIO.barcode).",
)
@click.option(
    "--max-workers",
    type=int,
    required=False,
    default=multiprocessing.cpu_count(),
    help="Determines how many subprocesses can be run in parallel.",
)
@needs_pipeline_ready
def step01_get_simhash(
    simhash_shingle_width: int,
    overwrite: bool,
    start: int | None,
    end: int | None,
    max_workers: int,
):
    """
    Collection-level items deduplication, step 01:
    Generate a simhash for every item in the collection.

    Notes:
    - Skips entries that were already analyzed, unless instructed otherwise.
    """
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        items_count = BookIO.select().offset(start).limit(end).count()

        batch_max_size = get_batch_max_size(
            items_count=items_count,
            max_workers=max_workers,
        )

        books_buffer = []
        """ Single series of book of length batch_max_size """

        #
        # Create batches of books to process
        #
        for i, book in enumerate(
            BookIO.select().offset(start).limit(end).order_by(BookIO.barcode).iterator(),
            start=1,
        ):
            books_buffer.append(book)

            # Run if buffer is full or we've reached last item
            if len(books_buffer) >= batch_max_size or i >= items_count:
                batch = executor.submit(
                    process_books_batch,
                    books_buffer,
                    simhash_shingle_width,
                    overwrite,
                )

                futures.append(batch)
                books_buffer = []

        #
        # Analyze batches in parallel
        #
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                click.echo(traceback.format_exc())
                click.echo("Could not run simhash on scanned texts. Interrupting.")
                executor.shutdown(wait=False, cancel_futures=True)
                exit(1)


def process_books_batch(
    books: list[BookIO],
    simhash_shingle_width: int = DEFAULT_SIMHASH_SHINGLE_WIDTH,
    overwrite: bool = False,
) -> bool:
    """
    Generates simhashes for a set of books and saves them.

    Note:
    - simhash_shingle_width is automatically reduced for books in specific languages for better accuracy
    """
    entries_to_create = []
    entries_to_update = []

    for book in books:
        scanned_text_simhash = None
        already_exists = False
        adjusted_simhash_shingle_width = simhash_shingle_width

        # Check if record already exists
        try:
            # throws if not found
            scanned_text_simhash = ScannedTextSimhash.get(book=book.barcode)
            assert scanned_text_simhash
            already_exists = True

            if already_exists and not overwrite:
                click.echo(f"⏭️ #{book.barcode} already analyzed.")
                continue
        except Exception:
            pass

        # Adjust `simhash_shingle_width` based on ISO 639 2B language code
        # NOTE: This list is likely incomplete
        if book.csv_data["gxml Language"] in SIMHASH_SHORT_SHINGLE_WIDTH_LANGUAGE:
            adjusted_simhash_shingle_width = 1

        # Prepare record
        scanned_text_simhash = ScannedTextSimhash() if not already_exists else scanned_text_simhash
        scanned_text_simhash.book = book.barcode

        merged_text = "\n".join(book.jsonl_data["text_by_page"])

        if merged_text.strip():

            hash = Simhash(
                get_simhash_features(
                    merged_text,
                    adjusted_simhash_shingle_width,
                )
            )
            scanned_text_simhash.hash = hash.value
            click.echo(f"🧮 #{book.barcode} = {hash.value}")
        else:
            click.echo(f"🧮 #{book.barcode} does not have text.")

        # Add to batch
        if already_exists:
            entries_to_update.append(scanned_text_simhash)
        else:
            entries_to_create.append(scanned_text_simhash)

    # Save batches
    process_db_write_batch(
        ScannedTextSimhash,
        entries_to_create,
        entries_to_update,
        [ScannedTextSimhash.hash],
    )
