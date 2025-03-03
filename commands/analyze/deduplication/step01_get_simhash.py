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


@click.command("step01-get-simhash")
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
    pass
    #
    # Assemble batches
    #
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        batch_max_size = get_batch_max_size(
            items_count=BookIO.select().offset(start).limit(end).order_by(BookIO.barcode).count(),
            max_workers=max_workers,
        )

        books = []
        """ Single series of book of length batch_max_size """

        # Create batches of books to process
        for book in BookIO.select().offset(start).limit(end).order_by(BookIO.barcode).iterator():

            if len(books) <= batch_max_size:
                books.append(book)
            else:
                batch = executor.submit(process_books_batch, books, overwrite)
                books = []
                futures.append(batch)

        # Run in parallel
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                click.echo(traceback.format_exc())
                click.echo("Could not run simhash on scanned texts. Interrupting.")
                executor.shutdown(wait=False, cancel_futures=True)
                exit(1)


def process_books_batch(books: list[BookIO], overwrite: bool = False) -> bool:
    """
    Generates simhashes for a set of books and saves them.
    """
    entries_to_create = []
    entries_to_update = []

    for book in books:
        scanned_text_simhash = None
        already_exists = False

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

        # Prepare record
        scanned_text_simhash = ScannedTextSimhash() if not already_exists else scanned_text_simhash
        scanned_text_simhash.book = book.barcode

        merged_text = "\n".join(book.jsonl_data["text_by_page"])

        if merged_text.strip():
            hash = Simhash(get_simhash_features(merged_text))
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
