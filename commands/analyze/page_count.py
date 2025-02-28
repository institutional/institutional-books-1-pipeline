import os
import traceback
import multiprocessing

import click
import tiktoken

import utils
from models import BookIO, PageCount
import const


@click.command("page-count")
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
@utils.needs_pipeline_ready
def page_count(
    overwrite: bool,
    start: int | None,
    end: int | None,
):
    """
    Counts and saves the number of available pages of each record.

    Notes:
    - Skips texts that were already analyzed, unless instructed otherwise.
    """
    entries_to_create = []
    entries_to_update = []
    entries_batch_max_size = 10_000

    for book in BookIO.select().offset(start).limit(end).order_by(BookIO.barcode).iterator():
        page_count = None
        already_exists = False

        # Check if page count already exists
        # NOTE: That check is done on the fly so this process can be easily parallelized.
        try:
            # throws if not found
            page_count = PageCount.get(book=book.barcode)
            assert page_count
            already_exists = True

            if already_exists and not overwrite:
                click.echo(f"⏭️ #{book.barcode} page count already exists.")
                continue
        except Exception:
            pass

        # Prepare record
        page_count = PageCount() if not already_exists else page_count
        page_count.book = book.barcode
        page_count.count_from_ocr = len(book.jsonl_data["text_by_page"])
        page_count.count_from_metadata = 0

        try:
            page_count.count_from_metadata = int(book.csv_data["Page Count"])
        except:
            pass

        click.echo(f"🧮 #{book.barcode} = {page_count.count_from_ocr} pages.")

        # Add to batch
        if already_exists:
            entries_to_update.append(page_count)
        else:
            entries_to_create.append(page_count)

        # Empty batches every X row
        if len(entries_to_create) + len(entries_to_update) >= entries_batch_max_size:
            save_entries_batches(entries_to_create, entries_to_update)

    # Save remaining items from batches
    save_entries_batches(entries_to_create, entries_to_update)


def save_entries_batches(
    entries_to_create: list[PageCount],
    entries_to_update: list[PageCount],
) -> bool:
    """
    Saves batches of entries.
    """
    if entries_to_create:
        PageCount.bulk_create(entries_to_create, batch_size=1000)
        entries_to_create.clear()

    if entries_to_update:
        PageCount.bulk_update(
            entries_to_update,
            fields=[PageCount.count_from_ocr, PageCount.count_from_metadata],
            batch_size=1000,
        )
        entries_to_update.clear()
