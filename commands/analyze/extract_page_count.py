import click

import utils
from models import BookIO, PageCount


@click.command("extract-page-count")
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
    help="If set, allows for processing a subset of the whole issues batch (sorted by BookIO.barcode).",
)
@click.option(
    "--limit",
    type=int,
    required=False,
    help="If set, allows for processing a subset of the whole issues batch (sorted by BookIO.barcode).",
)
@click.option(
    "--db-write-batch-size",
    type=int,
    required=False,
    default=10_000,
    help="Determines the frequency at which records are pushed to the database. By default: once every 10,000 record creation/update request.",
)
@utils.needs_pipeline_ready
def extract_page_count(
    overwrite: bool,
    offset: int | None,
    limit: int | None,
    db_write_batch_size: int,
):
    """
    Extracts the page count of each book:
    - As expressed in the collection's metadata
    - From the total of available pages in the OCR'd text

    Notes:
    - Skips texts that were already analyzed, unless instructed otherwise
    """
    entries_to_create = []
    entries_to_update = []
    fields_to_update = [PageCount.count_from_ocr, PageCount.count_from_metadata]

    for book in BookIO.select().offset(offset).limit(limit).order_by(BookIO.barcode).iterator():
        page_count = None
        already_exists = False

        # Check if record already exists
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
        if len(entries_to_create) + len(entries_to_update) >= db_write_batch_size:
            utils.process_db_write_batch(
                PageCount,
                entries_to_create,
                entries_to_update,
                fields_to_update,
            )

    # Save remaining items from batches
    utils.process_db_write_batch(
        PageCount,
        entries_to_create,
        entries_to_update,
        fields_to_update,
    )
